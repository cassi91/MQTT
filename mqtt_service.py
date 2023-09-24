import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler
import paho.mqtt.client as mqtt


class MyService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'SmartPowerMqttService'
    _svc_display_name_ = 'Smart Power Handle Mqtt Service'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.is_alive = True

        # 设置日志记录器
        self._setup_logging()

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_alive = False

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def _setup_logging(self):
        log_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'my_service.log')
        log_handler = TimedRotatingFileHandler(log_filename, when='midnight', interval=1, backupCount=7)
        log_handler.suffix = '%Y-%m-%d.log'
        log_formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        log_handler.setFormatter(log_formatter)

        self.logger = logging.getLogger()
        self.logger.addHandler(log_handler)
        self.logger.setLevel(logging.INFO)

    def main(self):
        # 在这里放置你的Python服务代码，使用self.logger记录日志
        self.logger.info('Service started.')
        try:
            while self.is_alive:
                # 你的服务逻辑
                pass
        except Exception as e:
            self.logger.error(f'An error occurred: {str(e)}')
            # 处理错误

        self.logger.info('Service stopped.')


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MyService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MyService)
