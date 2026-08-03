import re
import time
from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional
from urllib.parse import urljoin

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.chain.site import SiteChain
from app.core.config import settings
from app.core.event import eventmanager
from app.db.site_oper import SiteOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType
from app.utils.site import SiteUtils
from app.utils.string import StringUtils
from app.utils.timer import TimerUtils


class SiteRefreshCDP(_PluginBase):
    # 插件名称
    plugin_name = "站点签到更新(CDP)"
    # 插件描述
    plugin_desc = "通过CDP连接外部浏览器签到，Cookie失效时自动登录并回写Cookie和UA。"
    # 插件图标
    plugin_icon = "Chrome_A.png"
    # 插件版本
    plugin_version = "1.2"
    # 插件作者
    plugin_author = "al"
    # 作者主页
    author_url = "https://github.com/al"
    # 插件配置项ID前缀
    plugin_config_prefix = "siterefreshcdp_"
    # 加载顺序
    plugin_order = 2
    # 可使用的用户级别
    auth_level = 2

    # 配置属性
    _enabled: bool = False
    _notify: bool = False
    _onlyonce: bool = False
    _cron: str = ""
    # CDP地址，例如 http://192.168.1.10:9222
    _cdp_url: str = ""
    # 参与签到的站点ID
    _sign_sites: list = []
    # 验证码OCR识别重试次数
    _ocr_retry: int = 3
    # CDP不可用时是否回落到主程序内置浏览器方案
    _fallback: bool = True
    # 单站点操作超时（秒）
    _timeout: int = 60
    """
    格式
    站点domain|用户名|用户密码(|两步验证码)
    """
    _siteconf: list = []

    _scheduler: Optional[BackgroundScheduler] = None
    siteoper: SiteOper = None

    def init_plugin(self, config: dict = None):
        self.siteoper = SiteOper()
        self.stop_service()

        # 配置
        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._cron = str(config.get("cron") or "").strip()
            self._cdp_url = str(config.get("cdp_url") or "").strip()
            self._sign_sites = config.get("sign_sites") or []
            self._fallback = config.get("fallback", True)
            try:
                self._ocr_retry = int(config.get("ocr_retry") or 3)
            except (TypeError, ValueError):
                self._ocr_retry = 3
            try:
                self._timeout = int(config.get("timeout") or 60)
            except (TypeError, ValueError):
                self._timeout = 60
            self._siteconf = str(config.get("siteconf") or "").split('\n')

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info("站点签到更新(CDP)服务，立即运行一次")
            self._scheduler.add_job(func=self.sign_in, trigger='date',
                                    run_date=datetime.now(
                                        tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3))
            self._onlyonce = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "cdp_url": self._cdp_url,
            "sign_sites": self._sign_sites,
            "fallback": self._fallback,
            "ocr_retry": self._ocr_retry,
            "timeout": self._timeout,
            "siteconf": "\n".join([s for s in self._siteconf if s])
        })

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        """
        if not self._enabled:
            return []
        if self._cron:
            try:
                if str(self._cron).strip().count(" ") == 4:
                    return [{
                        "id": "SiteRefreshCDP",
                        "name": "站点签到更新(CDP)服务",
                        "trigger": CronTrigger.from_crontab(self._cron),
                        "func": self.sign_in,
                        "kwargs": {}
                    }]
                return [{
                    "id": "SiteRefreshCDP",
                    "name": "站点签到更新(CDP)服务",
                    "trigger": "interval",
                    "func": self.sign_in,
                    "kwargs": {"hours": float(str(self._cron).strip())}
                }]
            except Exception as err:
                logger.error(f"定时任务配置错误：{err}")
                return []
        # 未配置周期时，每天随机两个时间点执行
        triggers = TimerUtils.random_scheduler(num_executions=2,
                                               begin_hour=9,
                                               end_hour=23,
                                               max_interval=6 * 60,
                                               min_interval=2 * 60)
        return [{
            "id": f"SiteRefreshCDP|{trigger.hour}:{trigger.minute}",
            "name": "站点签到更新(CDP)服务",
            "trigger": "cron",
            "func": self.sign_in,
            "kwargs": {"hour": trigger.hour, "minute": trigger.minute}
        } for trigger in triggers]

    def sign_in(self, event=None):
        """
        签到主流程：一次CDP连接跑完所有站点
        """
        if not self._enabled:
            return
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "siterefreshcdp":
                return

        if not self._cdp_url:
            logger.error("未配置CDP地址，无法执行")
            return
        if not self._sign_sites:
            logger.warn("未选择签到站点")
            return

        sites = [site for site in self.siteoper.list_order_by_pri()
                 if site.id in self._sign_sites]
        if not sites:
            logger.warn("选中的站点均不存在")
            return

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("未安装playwright，无法使用CDP")
            return

        logger.info(f"开始站点签到，共 {len(sites)} 个站点")
        results = []
        browser = None
        context = None
        own_context = False
        try:
            with sync_playwright() as p:
                logger.info(f"连接CDP浏览器 {self._cdp_url}")
                browser = p.chromium.connect_over_cdp(self._cdp_url,
                                                      timeout=self._timeout * 1000)
                # 复用外部浏览器已有上下文，能带上它的真实指纹和登录态
                if browser.contexts:
                    context = browser.contexts[0]
                else:
                    context = browser.new_context()
                    own_context = True

                for site in sites:
                    results.append(self.__signin_site(context=context, site=site))
        except Exception as e:
            logger.error(f"CDP连接异常：{e}")
            return
        finally:
            for closer in (context if own_context else None, browser):
                try:
                    if closer:
                        closer.close()
                except Exception:
                    pass

        # 汇总
        logger.info("站点签到任务完成")
        self.__save_history(results)
        if self._notify and results:
            text = "\n".join([f"{r.get('site')}：{r.get('status')}" for r in results])
            self.post_message(mtype=NotificationType.SiteMessage,
                              title="【站点签到更新(CDP)】",
                              text=text)

    def __save_history(self, results: List[dict]):
        """
        保存签到记录，仅保留最近100条
        """
        if not results:
            return
        try:
            history = self.get_data("sign_history") or []
            history.extend(results)
            self.save_data("sign_history", history[-100:])
        except Exception as e:
            logger.error(f"保存签到记录失败：{e}")

    def __signin_site(self, context, site) -> dict:
        """
        单站点签到，未登录时先登录再重试
        :return: {date, site, status, login, cookie}
        """
        site_name = site.name
        checkin_url = site.url if "attendance.php" in site.url \
            else urljoin(site.url, "attendance.php")
        record = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "site": site_name,
            "status": "",
            "login": "未触发",
            "cookie": "未更新"
        }
        page = None
        try:
            page = context.new_page()
            page.set_default_timeout(self._timeout * 1000)

            logger.info(f"开始站点签到：{site_name}，地址：{checkin_url}")
            page.goto(checkin_url)
            page.wait_for_load_state("domcontentloaded")
            html = page.content()

            # Cookie已失效时现场登录，省去等下次触发
            if not SiteUtils.is_logged_in(html):
                logger.warn(f"站点{site_name}未登录，尝试自动登录")
                if not self.__login(page=page, site=site):
                    record["login"] = "失败"
                    record["status"] = "签到失败，登录失败"
                    return record
                record["login"] = "成功"
                # 登录成功顺带回写Cookie和UA
                if self.__save_cookie(context=context, page=page, site=site):
                    record["cookie"] = "已更新"
                page.goto(checkin_url)
                page.wait_for_load_state("domcontentloaded")
                html = page.content()
                if not SiteUtils.is_logged_in(html):
                    record["status"] = "签到失败，登录后仍未通过"
                    return record
            else:
                # 登录态正常时也刷新一次Cookie，避免它临近过期
                if self.__save_cookie(context=context, page=page, site=site):
                    record["cookie"] = "已更新"

            if re.search(r'已签|签到已得', html, re.IGNORECASE) or SiteUtils.is_checkin(html):
                logger.info(f"{site_name} 签到成功")
                record["status"] = "签到成功"
            else:
                logger.info(f"{site_name} 已访问签到页")
                record["status"] = "已访问签到页"
            return record
        except Exception as e:
            logger.error(f"站点{site_name}签到异常：{e}")
            record["status"] = f"签到失败：{e}"
            return record
        finally:
            try:
                if page:
                    page.close()
            except Exception:
                pass

    def __login(self, page, site) -> bool:
        """
        在当前页面完成登录
        """
        username, password, two_step_code = self.__get_site_conf(site)
        if not (username and password):
            logger.error(f"未配置站点{site.name}的用户名密码，无法自动登录")
            return False

        login_url = urljoin(site.url, "login.php")
        try:
            page.goto(login_url)
            page.wait_for_load_state("domcontentloaded")
        except Exception as e:
            logger.error(f"站点{site.name}打开登录页失败：{e}")
            return False

        return self.__fill_and_submit(page=page,
                                      site_name=site.name,
                                      username=username,
                                      password=password,
                                      two_step_code=two_step_code)

    def __get_site_conf(self, site) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        从配置中匹配出站点的登录凭据
        """
        for site_conf in self._siteconf:
            if not site_conf:
                continue
            site_confs = str(site_conf).split("|")
            try:
                siteurl, siteuser, sitepwd, *sitecode = site_confs
                sitecode = str(sitecode[0]) if sitecode else ""
            except Exception as e:
                logger.error(f"{site_conf}配置有误:{e}，已跳过")
                continue
            if str(siteurl) in StringUtils.get_url_domain(site.url):
                return siteuser, sitepwd, sitecode
        return None, None, None

    def __fill_and_submit(self, page, site_name: str, username: str,
                          password: str, two_step_code: str = None) -> bool:
        """
        填写NexusPHP登录表单并提交，含验证码OCR识别重试
        """
        retry = max(1, self._ocr_retry)
        for i in range(retry):
            try:
                page.fill('input[name="username"]', username)
                page.fill('input[name="password"]', password)

                # 两步验证码，站点未开启时该字段不存在
                if two_step_code and page.locator('input[name="scode"]').count():
                    page.fill('input[name="scode"]', two_step_code)

                # 图形验证码，站点未开启时该字段不存在
                if page.locator('input[name="imagestring"]').count():
                    captcha = self.__ocr_captcha(page=page, site_name=site_name)
                    if not captcha:
                        return False
                    page.fill('input[name="imagestring"]', captcha)

                # 高级选项一律不勾：自动登出和限制IP都会让Cookie很快失效
                for name in ("logout", "securelogin"):
                    box = page.locator(f'input[name="{name}"]')
                    if box.count() and box.is_checked():
                        box.uncheck()

                page.click('input[type="submit"]')
                page.wait_for_load_state("domcontentloaded")
                time.sleep(2)

                if SiteUtils.is_logged_in(page.content()):
                    logger.info(f"站点{site_name}登录成功")
                    return True

                if i < retry - 1:
                    logger.warn(f"站点{site_name}登录未通过，第{i + 2}次重试")
                    page.goto(urljoin(page.url, "login.php"))
                    page.wait_for_load_state("domcontentloaded")
            except Exception as e:
                logger.error(f"站点{site_name}填写登录表单失败：{e}")
                return False

        logger.error(f"站点{site_name}登录失败，已重试{retry}次")
        return False

    def __ocr_captcha(self, page, site_name: str) -> Optional[str]:
        """
        截取验证码图片并OCR识别
        """
        try:
            import ddddocr
        except ImportError:
            logger.error("未安装ddddocr，无法识别验证码。请确认插件依赖已安装")
            return None

        try:
            img = page.locator('img[alt="CAPTCHA"]')
            if not img.count():
                # 部分站点模板没有alt，退回按src匹配
                img = page.locator('img[src*="action=regimage"]')
            if not img.count():
                logger.error(f"站点{site_name}未定位到验证码图片")
                return None

            image_bytes = img.first.screenshot()
            code = ddddocr.DdddOcr(show_ad=False).classification(image_bytes)
            if not code:
                logger.error(f"站点{site_name}验证码识别为空")
                return None
            logger.info(f"站点{site_name}验证码识别结果：{code}")
            return code
        except Exception as e:
            logger.error(f"站点{site_name}验证码识别失败：{e}")
            return None

    def __save_cookie(self, context, page, site) -> bool:
        """
        从浏览器提取Cookie和UA并回写站点
        """
        try:
            domain = StringUtils.get_url_domain(site.url)
            items = [f"{c.get('name')}={c.get('value')}"
                     for c in context.cookies()
                     if domain in str(c.get("domain") or "")]
            if not items:
                logger.warn(f"站点{site.name}未提取到Cookie")
                return False
            cookie = "; ".join(items)
            ua = page.evaluate("() => navigator.userAgent")
            self.siteoper.update(site.id, {"cookie": cookie, "ua": ua})
            logger.info(f"站点{site.name}的Cookie和UA已更新")
            return True
        except Exception as e:
            logger.error(f"站点{site.name}回写Cookie失败：{e}")
            return False

    @eventmanager.register(EventType.PluginAction)
    def site_refresh(self, event):
        """
        兼容【站点自动签到】插件在Cookie失效时发出的刷新事件
        """
        if not self.get_state():
            return
        if not event:
            return
        event_data = event.event_data
        if not event_data or event_data.get("action") != "site_refresh":
            return
        site_id = event_data.get("site_id")
        if not site_id:
            logger.error("未获取到site_id")
            return

        site = self.siteoper.get(site_id)
        if not site:
            logger.error(f"未获取到site_id {site_id} 对应的站点数据")
            return

        username, password, two_step_code = self.__get_site_conf(site)
        if not (username and password):
            logger.error(f"未获取到站点{site.name}配置，已跳过")
            return

        state = False
        if self._cdp_url:
            state = self.__refresh_by_cdp(site=site)
        if not state and self._fallback:
            logger.warn(f"站点{site.name} CDP登录失败，回落到内置浏览器方案")
            state, _ = SiteChain().update_cookie(site_info=site,
                                                 username=username,
                                                 password=password,
                                                 two_step_code=two_step_code)

        if state:
            logger.info(f"站点{site.name}自动更新Cookie和Ua成功")
        else:
            logger.error(f"站点{site.name}自动更新Cookie和Ua失败")

        if self._notify:
            self.post_message(mtype=NotificationType.SiteMessage,
                              title=f"站点 {site.name} Cookie已失效。",
                              text=f"自动更新Cookie和Ua{'成功' if state else '失败'}")

    def __refresh_by_cdp(self, site) -> bool:
        """
        仅登录并回写Cookie，不签到
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.error("未安装playwright，无法使用CDP登录")
            return False

        browser = None
        context = None
        page = None
        own_context = False
        try:
            with sync_playwright() as p:
                browser = p.chromium.connect_over_cdp(self._cdp_url,
                                                      timeout=self._timeout * 1000)
                if browser.contexts:
                    context = browser.contexts[0]
                else:
                    context = browser.new_context()
                    own_context = True
                page = context.new_page()
                page.set_default_timeout(self._timeout * 1000)

                if not self.__login(page=page, site=site):
                    return False
                return self.__save_cookie(context=context, page=page, site=site)
        except Exception as e:
            logger.error(f"站点{site.name} CDP登录异常：{e}")
            return False
        finally:
            # 只清理自己创建的资源，不动用户浏览器里已有的标签页
            for closer in (page, context if own_context else None, browser):
                try:
                    if closer:
                        closer.close()
                except Exception:
                    pass

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{
            "cmd": "/cdp_signin",
            "event": EventType.PluginAction,
            "desc": "站点签到更新(CDP)",
            "category": "站点",
            "data": {"action": "siterefreshcdp"}
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        site_options = [{"title": site.name, "value": site.id}
                        for site in SiteOper().list_order_by_pri()]
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'enabled', 'label': '启用插件'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'notify', 'label': '开启通知'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'onlyonce', 'label': '立即运行一次'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'fallback', 'label': '失败回落内置浏览器'}
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cdp_url',
                                            'label': 'CDP地址',
                                            'placeholder': 'http://192.168.1.10:9222'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '5位cron表达式，留空则每天随机执行两次'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'chips': True,
                                            'multiple': True,
                                            'model': 'sign_sites',
                                            'label': '签到站点',
                                            'items': site_options
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'ocr_retry',
                                            'label': '验证码重试次数',
                                            'placeholder': '3'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 6},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'timeout',
                                            'label': '超时时间（秒）',
                                            'placeholder': '60'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'siteconf',
                                            'label': '登录凭据',
                                            'rows': 5,
                                            'placeholder': '每一行一个站点，配置方式：\n'
                                                           '域名domain|用户名|用户密码(|二次验证秘钥)\n'
                                                           '只有需要自动登录的站点才需要配置\n'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '按周期访问签到页，Cookie失效时自动登录并回写Cookie和UA，无需依赖【站点自动签到】插件。'
                                                    '仅适配NexusPHP标准签到与登录表单，验证码由ddddocr识别，不保证成功率。'
                                                    '未配置登录凭据的站点，Cookie失效时只会记录失败。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "fallback": True,
            "cron": "",
            "cdp_url": "",
            "sign_sites": [],
            "ocr_retry": 3,
            "timeout": 60,
            "siteconf": ""
        }

    def get_page(self) -> List[dict]:
        """
        拼装插件详情页面，展示最近的签到与Cookie更新记录
        """
        history = self.get_data("sign_history") or []
        # 最近的排在最前
        history = list(reversed(history))[:50]

        if history:
            contents = [
                {
                    'component': 'tr',
                    'props': {'class': 'text-sm'},
                    'content': [
                        {
                            'component': 'td',
                            'props': {'class': 'whitespace-nowrap break-keep text-high-emphasis'},
                            'text': record.get("date")
                        },
                        {
                            'component': 'td',
                            'props': {'class': 'whitespace-nowrap break-keep'},
                            'text': record.get("site")
                        },
                        {
                            'component': 'td',
                            'text': record.get("status")
                        },
                        {
                            'component': 'td',
                            'text': record.get("login")
                        },
                        {
                            'component': 'td',
                            'text': record.get("cookie")
                        }
                    ]
                } for record in history
            ]
        else:
            contents = [
                {
                    'component': 'tr',
                    'props': {'class': 'text-sm'},
                    'content': [
                        {
                            'component': 'td',
                            'props': {'colspan': 5, 'class': 'text-center'},
                            'text': '暂无数据'
                        }
                    ]
                }
            ]

        return [
            {
                'component': 'VTable',
                'props': {'hover': True},
                'content': [
                    {
                        'component': 'thead',
                        'content': [
                            {
                                'component': 'th',
                                'props': {'class': 'text-start ps-4'},
                                'text': '时间'
                            },
                            {
                                'component': 'th',
                                'props': {'class': 'text-start ps-4'},
                                'text': '站点'
                            },
                            {
                                'component': 'th',
                                'props': {'class': 'text-start ps-4'},
                                'text': '签到状态'
                            },
                            {
                                'component': 'th',
                                'props': {'class': 'text-start ps-4'},
                                'text': '自动登录'
                            },
                            {
                                'component': 'th',
                                'props': {'class': 'text-start ps-4'},
                                'text': 'Cookie'
                            }
                        ]
                    },
                    {
                        'component': 'tbody',
                        'content': contents
                    }
                ]
            }
        ]

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"退出插件失败：{e}")
