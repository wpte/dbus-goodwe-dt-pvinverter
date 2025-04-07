#!/usr/bin/env python
import platform 
import logging
import sys
import os
import sys
import dbus
import dbus.service
if sys.version_info.major == 2:
    import gobject
else:
    from gi.repository import GLib as gobject
import sys
import time
import configparser # for config/ini file
 
# goodwe library and asyncio
import asyncio
import goodwe as goodwe

# our own packages from victron
sys.path.insert(1, os.path.join(os.path.dirname(__file__), '/opt/victronenergy/dbus-systemcalc-py/ext/velib_python'))
from vedbus import VeDbusService

class SystemBus(dbus.bus.BusConnection):
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM)
 
class SessionBus(dbus.bus.BusConnection):
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SESSION)

class VictronDbusService():
  """ VictronDbusService holds VDbus specific service creation and connection code
  """
  def _dbus_connection(self):
    return SessionBus() if 'DBUS_SESSION_BUS_ADDRESS' in os.environ else SystemBus()
 
  # Here is the bit you need to create multiple new services - try as much as possible timplement the Victron Dbus API requirements.
  def create_dbus_service(self, base, physical, logical, id, instance, product_id, product_name, custom_name, type=None):
    dbus_service = VeDbusService("{}.{}.{}_id{:02d}".format(base, type, physical, id), self._dbus_connection(), register=False)

    # physical is the physical connection
    # logical is the logical connection to align with the numbering of the console display
    # Create the management objects, as specified in the ccgx dbus-api document
    dbus_service.add_path('/Mgmt/ProcessName', __file__)
    dbus_service.add_path('/Mgmt/ProcessVersion', 'Unknown version, and running on Python ' + platform.python_version())
    dbus_service.add_path('/Mgmt/Connection', logical)

    # Create the mandatory objects, note these may need to be customized after object creation
    # We're creating a connected object by default
    dbus_service.add_path('/DeviceInstance', instance)
    dbus_service.add_path('/ProductId', product_id)
    dbus_service.add_path('/ProductName', product_name)
    dbus_service.add_path('/CustomName', custom_name)
    dbus_service.add_path('/FirmwareVersion', 0)
    dbus_service.add_path('/HardwareVersion', 0)
    dbus_service.add_path('/Connected', 1, writeable=True)  # Mark devices as disconnected until they are confirmed

    dbus_service.add_path('/UpdateIndex', 0, writeable=True)
    dbus_service.add_path('/StatusCode', 0, writeable=True)

    # Create device type specific objects
    if type == 'temperature':
        dbus_service.add_path('/Temperature', 0)
        dbus_service.add_path('/Status', 0)
        dbus_service.add_path('/TemperatureType', 0, writeable=True)
    if type == 'humidity':
        dbus_service.add_path('/Humidity', 0)
        dbus_service.add_path('/Status', 0)

    # Register the service after adding all paths
    dbus_service.register()

    return dbus_service

class GoodWeEMService:
    """ GoodWe Inverter and SmartMeter class
    """
    def __init__(self, product_name='GoodWe EM', connection='GoodWe EM service'):
        """Creates a GoodWeEMService object to interact with GoodWe Inverter and SmartMeter, 
        it also handles configuration management and Dbus updates

        Args:
            product_name (str, optional): _description_. Defaults to 'GoodWe EM'.
            connection (str, optional): _description_. Defaults to 'GoodWe EM service'.
        """
        config = self._get_config()

        self.dbus_service = None
        self.custom_name = config['DEFAULT']['CustomName']
        self.product_name = product_name
        self.product_id = 0xFFFF
        self.logical_connection = connection
        self.device_instance = int(config['DEFAULT']['DeviceInstance'])
        self.has_meter = bool(config['ONPREMISE']['HasMeter'])
        self.pv_inverter_position = int(config['ONPREMISE']['Position'])
        self.pv_max_power = int(config['ONPREMISE']['MaxPower'])
        self.pv_host = config['ONPREMISE']['Host']

        if self.has_meter:
            self.meter_product_name = config['SMARTMETER']['ProductName']

        # Initialize attributes
        self.pv_power = 0
        self.pv_current = 0
        self.e_total = 0
        self.pv_voltage = 0
        self.vgrid1 = 0
        self.vgrid2 = 0
        self.vgrid3 = 0
        self.igrid1 = 0
        self.igrid2 = 0
        self.igrid3 = 0
        self.pgrid1 = 0
        self.pgrid2 = 0
        self.pgrid3 = 0
        self.total_inverter_power = 0
        self.work_mode = 0

        #formatting 
        self._kwh = lambda p, v: (str(round(v, 2)) + 'KWh')
        self._a = lambda p, v: (str(round(v, 1)) + 'A')
        self._w = lambda p, v: (str(round(v, 1)) + 'W')
        self._v = lambda p, v: (str(round(v, 1)) + 'V') 

        logging.debug("%s /DeviceInstance = %d" % (self.custom_name, self.device_instance))

        # Initialize GoodWe inverter connection
        self.inverter = None

    def set_dbus_service(self, dbus_service):
        self.dbus_service = dbus_service

    def _get_config(self):
        config = configparser.ConfigParser()
        config.read("%s/config.ini" % (os.path.dirname(os.path.realpath(__file__))))
        return config

    async def _ping_host(self):
        proc = await asyncio.create_subprocess_shell(
            f"ping -c 1 -t 2 -W 2 {self.pv_host}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        return proc.returncode == 0

    async def _connect_to_inverter(self):
        if await self._ping_host():
            try:
                self.inverter = await goodwe.connect(self.pv_host)
            except Exception as e:
                logging.error("Failed to connect to GoodWe inverter: %s", e)
                self.inverter = None
        else:
            logging.info("Host %s is not reachable", self.pv_host)
            self.inverter = None

    async def _get_goodwe_data(self):
        if self.inverter is None:
            await self._connect_to_inverter()

        if self.inverter is not None:
            try:
                meter_data = await self.inverter.read_runtime_data()
                return meter_data
            except Exception as e:
                logging.error("Failed to read data from GoodWe inverter: %s", e)
                self.inverter = None

        return {}

    def refresh_meter_data(self):
        try:
            # get data from GoodWe EM through Goodwe python library in async
            meter_data = asyncio.run(self._get_goodwe_data())

            if not meter_data:
                # If meter_data is empty, set all variables to 0 except e_total to keep track of total energy
                self.pv_power = 0
                self.pv_current = 0
                self.pv_voltage = 0
                self.vgrid1 = 0
                self.vgrid2 = 0
                self.vgrid3 = 0
                self.igrid1 = 0
                self.igrid2 = 0
                self.igrid3 = 0
                self.pgrid1 = 0
                self.pgrid2 = 0
                self.pgrid3 = 0
                self.work_mode = 0  # Standby mode
            else:
                # ppv = for photo voltaic voltage
                self.pv_power = meter_data.get('ppv', 0)
                # igrid current ac on grid (not differentiated by the meter)
                self.pv_current = meter_data.get('igrid', 0)
                # total power equals power as GoodWe gives us the aggregated amount
                self.e_total = meter_data.get('e_total', 0)
                # total voltage on AC line (not differentiated by the meter)
                self.pv_voltage = meter_data.get('vgrid', 0)

                # 3-phase specific data
                self.vgrid1 = meter_data.get('vgrid1', 0)
                self.vgrid2 = meter_data.get('vgrid2', 0)
                self.vgrid3 = meter_data.get('vgrid3', 0)
                self.igrid1 = meter_data.get('igrid1', 0)
                self.igrid2 = meter_data.get('igrid2', 0)
                self.igrid3 = meter_data.get('igrid3', 0)
                self.pgrid1 = meter_data.get('pgrid1', 0)
                self.pgrid2 = meter_data.get('pgrid2', 0)
                self.pgrid3 = meter_data.get('pgrid3', 0)
                self.total_inverter_power = meter_data.get('total_inverter_power', 0)
                self.work_mode = meter_data.get('work_mode', 0)

        except Exception as e:
            logging.critical('Error at %s', '_update', exc_info=e)

        return True

    def map_work_mode_to_status_code(self, work_mode):
        """Maps GoodWe work mode to Victron status code"""
        mapping = {
            0: 8,  # Wait Mode -> Standby
            1: 7,  # Normal (On-Grid) -> Running
            2: 7,  # Normal (Off-Grid) -> Running
            3: 10, # Fault Mode -> Error
            4: 9,  # Flash Mode -> Boot loading
            5: 0   # Check Mode -> Startup 0
        }
        return mapping.get(work_mode, 0)  # Default to Startup 0 if not found

    def update_dbus_pv_inverter(self):
        """_summary_
        updates dbus as a callback function, dbus is setted on the GoodWe EM Class
        Returns:
            _type_: _description_
        """
        dbus_service = self.dbus_service
        try:
            self.refresh_meter_data()
            # Update L1
            dbus_service['pvinverter']['/Ac/L1/Voltage'] = self.vgrid2
            dbus_service['pvinverter']['/Ac/L1/Current'] = self.igrid2
            dbus_service['pvinverter']['/Ac/L1/Power'] = self.pgrid2
            dbus_service['pvinverter']['/Ac/L1/Energy/Forward'] = self.e_total / 3
            # Update L2
            dbus_service['pvinverter']['/Ac/L2/Voltage'] = self.vgrid3
            dbus_service['pvinverter']['/Ac/L2/Current'] = self.igrid3
            dbus_service['pvinverter']['/Ac/L2/Power'] = self.pgrid3
            dbus_service['pvinverter']['/Ac/L2/Energy/Forward'] = self.e_total / 3
            # Update L3
            dbus_service['pvinverter']['/Ac/L3/Voltage'] = self.vgrid1
            dbus_service['pvinverter']['/Ac/L3/Current'] = self.igrid1
            dbus_service['pvinverter']['/Ac/L3/Power'] = self.pgrid1
            dbus_service['pvinverter']['/Ac/L3/Energy/Forward'] = self.e_total / 3

            # Update total values
            dbus_service['pvinverter']['/Ac/Power'] = self.total_inverter_power
            dbus_service['pvinverter']['/Ac/Energy/Forward'] = self.e_total

            # Update status based on work_mode
            dbus_service['pvinverter']['/StatusCode'] = self.map_work_mode_to_status_code(self.work_mode)

            #logging
            logging.debug("House Consumption (/Ac/Power): %s" % (dbus_service['pvinverter']['/Ac/Power']))
            logging.debug("House Forward (/Ac/Energy/Forward): %s" % (dbus_service['pvinverter']['/Ac/Energy/Forward']))
            logging.debug("---")
            
            # increment UpdateIndex - to show that new data is available
            index = dbus_service['pvinverter']['/UpdateIndex'] + 1  # increment index
            if index > 255:   # maximum value of the index
                index = 0       # overflow from 255 to 0
            dbus_service['pvinverter']['/UpdateIndex'] = index

            #update lastupdate vars
            self._dbus_last_update = time.time()    
        except Exception as e:
            logging.critical('Error at %s', '_update', exc_info=e)

        return True

def main():
    #configure logging
    logging.basicConfig(      format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S',
                              level=logging.INFO,
                              handlers=[
                                  logging.FileHandler("%s/current.log" % (os.path.dirname(os.path.realpath(__file__)))),
                                  logging.StreamHandler()
                              ])

    logging.info("Start")

    from dbus.mainloop.glib import DBusGMainLoop
    # Have a mainloop, so we can send/receive asynchronous calls to and from dbus
    DBusGMainLoop(set_as_default=True)
    
    goodwe_inverter = GoodWeEMService()
    victron_dbus = VictronDbusService()

    try:
        # Dictionary to hold the multiple services as we have two dbus but only one outgoing http connection
        dbusservice = {} # 
  
        # Base dbus path
        base = 'com.victronenergy'

        # creating new dbus service on pvinverter path
        dbusservice['pvinverter'] = victron_dbus.create_dbus_service(base, 'http', goodwe_inverter.logical_connection, 
        goodwe_inverter.device_instance, instance=goodwe_inverter.device_instance, product_id=goodwe_inverter.product_id,
        product_name=goodwe_inverter.product_name, custom_name=goodwe_inverter.custom_name, type="pvinverter"  )

        # add paths specific to pv inverter
        dbusservice['pvinverter'].add_path('/Ac/Energy/Forward', None, writeable=True, gettextcallback = goodwe_inverter._kwh)
        dbusservice['pvinverter'].add_path('/Ac/Power', 0, writeable=True, gettextcallback = goodwe_inverter._w)
        dbusservice['pvinverter'].add_path('/Ac/Current', 0, writeable=True, gettextcallback = goodwe_inverter._a)
        dbusservice['pvinverter'].add_path('/Ac/Voltage', 0, writeable=True, gettextcallback = goodwe_inverter._v)
        dbusservice['pvinverter'].add_path('/Ac/L1/Voltage', 0, writeable=True, gettextcallback = goodwe_inverter._v)
        dbusservice['pvinverter'].add_path('/Ac/L1/Current', 0, writeable=True, gettextcallback = goodwe_inverter._a)
        dbusservice['pvinverter'].add_path('/Ac/L1/Power', 0, writeable=True, gettextcallback = goodwe_inverter._w)
        dbusservice['pvinverter'].add_path('/Ac/L1/Energy/Forward', None, writeable=True, gettextcallback = goodwe_inverter._kwh)
        dbusservice['pvinverter'].add_path('/Ac/L2/Voltage', 0, writeable=True, gettextcallback = goodwe_inverter._v)
        dbusservice['pvinverter'].add_path('/Ac/L2/Current', 0, writeable=True, gettextcallback = goodwe_inverter._a)
        dbusservice['pvinverter'].add_path('/Ac/L2/Power', 0, writeable=True, gettextcallback = goodwe_inverter._w)
        dbusservice['pvinverter'].add_path('/Ac/L2/Energy/Forward', None, writeable=True, gettextcallback = goodwe_inverter._kwh)
        dbusservice['pvinverter'].add_path('/Ac/L3/Voltage', 0, writeable=True, gettextcallback = goodwe_inverter._v)
        dbusservice['pvinverter'].add_path('/Ac/L3/Current', 0, writeable=True, gettextcallback = goodwe_inverter._a)
        dbusservice['pvinverter'].add_path('/Ac/L3/Power', 0, writeable=True, gettextcallback = goodwe_inverter._w)
        dbusservice['pvinverter'].add_path('/Ac/L3/Energy/Forward', None, writeable=True, gettextcallback = goodwe_inverter._kwh)
        # Position is required to establish on which line the inverter sits (AC OUT, In, ETC)
        dbusservice['pvinverter'].add_path('/Position', goodwe_inverter.pv_inverter_position, writeable=True)
        dbusservice['pvinverter'].add_path('/MaxPower', goodwe_inverter.pv_max_power, writeable=True)
            
        # pass dbus object to goodwe class
        goodwe_inverter.set_dbus_service(dbusservice)
        # add _update function 'timer'
        # update every 5 seconds to prevent blocking by GoodWe Inverter
        gobject.timeout_add(5000, goodwe_inverter.update_dbus_pv_inverter) # pause 5000ms before the next request

        logging.info('Connected to dbus, and switching over to gobject.MainLoop() (= event based)')
        mainloop = gobject.MainLoop()
        mainloop.run()            
    except Exception as e:
        logging.critical('Error at %s', 'main', exc_info=e)

if __name__ == "__main__":
    main()
