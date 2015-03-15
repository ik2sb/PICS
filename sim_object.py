import sys
import inspect
import time
import math
import sim_lib_common as clib
import sim_global_variables as simgval
from threading import Condition

g_VM_Obj_CV = Condition()
g_Job_Obj_CV = Condition()
g_Config_Obj_CV = Condition()

g_JOB_UID = 0
g_JOB_UID_CV = Condition()

g_VM_UID = 0
g_VM_UID_CV = Condition()

g_SC_UID = 0
g_SC_UID_CV = Condition()

g_SFO_UID = 0
g_SFO_UID_CV = Condition()

class Job:

    def __init__ (self, job_id, job_infos, job_create_clock):

        #job_name, job_std_duration, job_deadline

        j_name = job_infos[0]           # job name
        j_std_duration = job_infos[1]   # job standard duration
        j_deadline = job_infos[2]       # job deadline

        j_if_direction = job_infos[3]   # job input file direction
        j_if_size = job_infos[4]        # job input file size

        j_of_direction = job_infos[5]   # job output file direction
        j_of_size = job_infos[6]        # job output file size

        # basic info
        self.id = job_id
        self.name = j_name
        self.std_duration = j_std_duration
        self.deadline = j_deadline

        # job status
        self.status = simgval.gJOB_ST_GENERATED
        # job status state transition: GENERATED -> ASSIGNED -> STARTED -> COMPLETED

        # Time Info
        self.time_create = job_create_clock
        self.time_assign = None
        self.time_start = None
        self.time_complete = None

        # VM Info
        self.VM_id = None
        self.VM_type = None
        self.act_duration_on_VM = 0      # total actual runtime on particular vm
        self.act_cputime_on_VM = 0      # total actual cputime on partucular vm
        self.act_nettime_on_VM = [0,0]     # total actual nettime on partucular vm (list - [0]: in, [1]: out)

        # Input File Info
        self.use_input_file = False
        self.input_file_flow_direction = simgval.gIOFTD_NONE
        self.input_file_size = 0            # unit KB
        self.input_file_transfer_status = None
        self.input_file_transfer_size = 0     # unit KB

        if j_if_direction == simgval.gIFTD_IC or j_if_direction == simgval.gIFTD_OC:

            self.use_input_file = True
            self.input_file_flow_direction = j_if_direction
            self.input_file_size = j_if_size

        # Output File Info
        self.use_output_file = False
        self.output_file_flow_direction = simgval.gIOFTD_NONE
        self.output_file_size = 0                   # unit KB
        self.output_file_transfer_status = None                 # newly added on 10272014 - storage simulation
        self.output_file_transfer_size = 0          # unit KB   # newly added on 10272014 - storage simulation

        if j_of_direction == simgval.gOFTD_IC or j_of_direction == simgval.gOFTD_OC:

            self.use_output_file = True
            self.output_file_flow_direction = j_of_direction
            self.output_file_size = j_of_size

        # job failure
        self.std_job_failure_time = 0
        self.vm_job_failure_time = 0
        self.job_failure_recovered = True

        if job_infos[7] > 0.0:
            self.std_job_failure_time = job_infos[7]
            self.job_failure_recovered = False

        # mgmt var
        #self.cv = Condition()

    def logwrite_job_obj (self, clock, logger):

        logger.info ("")
        logger.info ("%d ----------------------------------------" % clock)
        logger.info ("%d Display Job Obj" % clock)
        logger.info ("%d ----------------------------------------" % clock)
        logger.info ("%d Job ID                        = %d" % (clock, self.id))
        logger.info ("%d Job Name                      = %s" % (clock, self.name))
        logger.info ("%d Job Standard Duration         = %d" % (clock, self.std_duration))
        logger.info ("%d Job Deadline                  = %d" % (clock, self.deadline))
        logger.info ("%d Job Status                    = %s" % (clock, simgval.get_sim_code_str(self.status)))
        logger.info ("%d Job Time Create               = %s" % (clock, str(self.time_create)))
        logger.info ("%d Job Time Assign               = %s" % (clock, str(self.time_assign)))
        logger.info ("%d Job Time Start                = %s" % (clock, str(self.time_start)))
        logger.info ("%d Job Time Complete             = %s" % (clock, str(self.time_complete)))
        logger.info ("%d Job VM ID                     = %s" % (clock, str(self.VM_id)))
        logger.info ("%d Job VM Type                   = %s" % (clock, str(self.VM_type)))
        logger.info ("%d Job Actual Duration on VM     = %s" % (clock, str(self.act_duration_on_VM)))
        logger.info ("%d Job Actual CPU Time on VM     = %s" % (clock, str(self.act_cputime_on_VM)))
        logger.info ("%d Job Actual Network Time on VM = %s" % (clock, str(self.act_nettime_on_VM)))
        logger.info ("%d Job Use Input File            = %s" % (clock, str(self.use_input_file)))
        logger.info ("%d Job Input File Flow Direction = %s" % (clock, simgval.get_sim_code_str(self.input_file_flow_direction)))
        logger.info ("%d Job Input File Size           = %s KB (%s MB, %s GB, %s TB)" % (clock, str(self.input_file_size), str(self.input_file_size/1024.0), str(self.input_file_size/(1024.0*1024)), str(round(self.input_file_size/(1024.0*1024*1024), 5))))
        logger.info ("%d Job Use Output File           = %s" % (clock, str(self.use_output_file)))
        logger.info ("%d Job Output File Flow Direction= %s" % (clock, simgval.get_sim_code_str(self.output_file_flow_direction)))
        logger.info ("%d Job Output File Size          = %s KB (%s MB, %s GB, %s TB)" % (clock, str(self.output_file_size), str(self.output_file_size/1024.0), str(self.output_file_size/(1024.0*1024)), str(round(self.output_file_size/(1024.0*1024*1024), 5))))
        logger.info ("%d Job Output File Trans Status  = %s" % (clock, simgval.get_sim_code_str(self.output_file_transfer_status)))
        logger.info ("%d Job Output File Trans Size    = %s KB (%s MB, %s GB, %s TB)" % (clock, str(self.output_file_transfer_size), str(self.output_file_transfer_size/1024.0), str(self.output_file_transfer_size/(1024.0*1024)), str(round(self.output_file_transfer_size/(1024.0*1024*1024), 5))))
        logger.info ("%d Standard Job Failure Time     = %s" % (clock, str(self.std_job_failure_time)))
        logger.info ("%d VM Job Failure Time           = %s" % (clock, str(self.vm_job_failure_time)))
        logger.info ("%d Job Failure Recovered         = %s" % (clock, str(self.job_failure_recovered)))
        logger.info ("%d ----------------------------------------" % clock)
        logger.info ("")

class VM_Instance:

    def __init__ (self, id, instance_id, vm_type_name, vm_unit_price, create_clock):

        #basic info
        self.id = id
        self.instance_id = instance_id
        self.type_name = vm_type_name
        self.unit_price = vm_unit_price
        self.status = simgval.gVM_ST_CREATING
        # STATUS TRANSITION: CREATING -> ACTIVE -> TERMINATE

        #time info
        self.time_create = create_clock
        self.time_active = None # after startup delay
        self.time_terminate = None

        #cost and startup delay
        self.cost = 0
        self.startup_lag = 0
        
        # Job Info
        self.current_job = None
        self.current_job_start_clock = -1
        self.last_job_complete_clock = -1
        self.job_queue = []                 # job queue that has list of Job Class
        self.job_queue_cv = Condition()     # job queue cv
        self.job_history = []               # job complete list
        self.job_history_cv = Condition()   # job complete list cv

        self.sd_policy_activated = False    # Special Flag for some SD policies such as SL-SD, MEAN-JAT-SD, MAX-JAT-SD
        self.sd_wait_time = 0               # Wait time before scale down
                                            # this field is used by SL-SD, MEAN-JAT-SD, MAX-JAT-SD
        self.is_vscale_victim = False       # if this is true --> this is victim for vscale
        self.vscale_case = None             # either gVSO_VSCALE_UP or gVSO_VSCALE_DOWN
        self.vscale_victim_id = -1          # if this is not -1 --> this is new vscale result


    def get_str_info (self):
        return "VM (ID:%d, TY:%s, PR:%s, ST:%s)" % (self.id, self.type_name, self.unit_price, self.status)

class Config:

    def __init__ (self, id):

        # basic info
        self.id = id

        # Simulator Config
        self.sim_trace_interval = -1
        self.g_debug_log = None

        # log/report path
        self.log_path = None
        self.report_path = None

        # Workload
        self.workload_file = None   # file path
        self.workload_type = None   # type

        # startup lag
        self.min_startup_lag = None
        self.max_startup_lag = None

        # job_assignment policy and scaling up
        self.job_assign_policy = None
        self.job_assign_max_running_vms = None
        
        # vertical scaling
        self.enable_vertical_scaling = False
        self.vscale_operation= None

        # VM scale down policy
        self.vm_scale_down_policy = None
        self.vm_scale_down_policy_unit = None
        self.vm_scale_down_policy_recent_sample_cnt = 0
        self.vm_scale_down_policy_param1 = None
        self.vm_scale_down_policy_param2 = None
        self.vm_scale_down_policy_param3 = None
        self.vm_scale_down_policy_timer_unit = None
        self.vm_scale_down_policy_min_wait_time = 1
        self.vm_scale_down_policy_max_wait_time = sys.maxint

        # Pricing Policy
        #self.vm_unit_price = None -- this is diabled. by 10072014
        self.billing_time_unit = None       # for vm
        self.billing_time_period = None     # for vm
        self.billing_unit_clock = None      # for vm

        # VM lists
        self.vm_config_file_path = None
        self.vm_selection_method = None
        self.no_of_vm_types = 0
        self.vm_types = []

        # storage configuration
        self.max_capacity_of_storage = 0          # this is max capacity (unit megabytes)
        self.max_num_of_storage_containers = 100  # this is reserved
        self.storage_unit_cost = 0.0              # this is cost for storage: $$$/GB
        self.storage_billing_unit_clock = 0       # this is billing time unit for storage -- Month is default (based on AWS)

        # Data Transfer Model
        # perf_data_transfer = ['CLOUD', 'IN', 'OUT'] -- XX MB/s
        # cost_data_transfer = ['CLOUD', 'IN', 'OUT'] -- $$ /GB
        self.perf_data_transfer = []
        self.cost_data_transfer = []

        # job/application failure model
        self.prob_job_failure = 0.0
        self.job_failure_policy = None

    def get_vm_types_str (self):

        ret_str = "["

        for x in xrange (self.no_of_vm_types):
            ret_str += self.vm_types[x].get_infos()
            ret_str += ", "

        ret_str += "]"

        return ret_str

class VM_Type:

    def __init__ (self, id):

        self.vm_type_id = id
        self.vm_type_name = None
        self.vm_type_unit_price = 0
        self.vm_type_cpu_factor = 0
        self.vm_type_net_factor = 0

    def set_vm_type_name (self, name):
        self.vm_type_name = name

    def set_vm_type_unit_price (self, price):
        self.vm_type_unit_price = price

    def set_vm_type_cpu_factor (self, factor):
        self.vm_type_cpu_factor = factor

    def set_vm_type_net_factor (self, factor):
        self.vm_type_net_factor = factor

    def get_infos (self):
        return  "<ID:" + str(self.vm_type_id) + ", NM:" + str(self.vm_type_name) + ", PR:$" + str(self.vm_type_unit_price) + ", CF:" + str(self.vm_type_cpu_factor) + ", NF:" + str(self.vm_type_net_factor) + ">"

class Storage_Container:

    def __init__ (self, id, creator, region, clock):

        self.sc_id = id
        self.sc_creator = creator
        self.sc_terminator = None
        self.sc_region = region
        self.sc_permission = None
        self.sc_status = simgval.gSC_ST_CREATE
        self.sc_create_time = clock
        self.sc_delete_time = None
        self.sc_usage_price = 0.0
        self.sc_usage_volume = 0 # unit is KB
        self.sc_file_objects = []
        self.sc_cv = Condition()

    def set_sc_terminator (self, terminator):

        self.sc_cv.acquire()
        self.sc_terminator = terminator
        self.sc_cv.notify()
        self.sc_cv.release()

    def set_sc_status (self, status):

        self.sc_cv.acquire()
        self.sc_status = status
        self.sc_cv.notify()
        self.sc_cv.release()

    def set_sc_permission (self, sc_permission):

        self.sc_cv.acquire()
        self.sc_permission = sc_permission
        self.sc_cv.notify()
        self.sc_cv.release()

    def set_sc_delete_time (self, clock):

        self.sc_cv.acquire()
        self.sc_delete_time = clock
        self.sc_cv.notify()
        self.sc_cv.release()

    def set_sc_usage_price (self, price):

        # this is for debug
        if self.sc_usage_price > price:
            clib.sim_exit()

        self.sc_cv.acquire()
        self.sc_usage_price = price
        self.sc_cv.notify()
        self.sc_cv.release()

    def delete_storage_container (self, clock, terminator):

        self.set_sc_status (simgval.gSC_ST_DELETED)
        self.set_sc_delete_time (clock)
        self.set_sc_terminator(terminator)


    def upload_file_start (self, creator, upload_start_clock, actual_file_size, planned_file_size):

        # create sfo
        sfo_uid = generate_SFO_UID()
        sfo_obj = Storage_File_Object (sfo_uid, self.sc_id, actual_file_size, creator, planned_file_size, upload_start_clock)

        # insert sfo obj into file list
        self.insert_sfo_into_sc_file_objects (sfo_obj)

        # update storage volume
        self.increase_sc_volume (actual_file_size)

        total_size_of_sfos = self.get_total_size_of_sfos()
        if self.sc_usage_volume != total_size_of_sfos:
            print "[SC Obj Debug] Error! - SC Volume Size Mismatch - OBJ (%s KB) != Calculated (%s KB)" % (self.sc_usage_volume, total_size_of_sfos)
            clib.sim_exit()

        ##########################################################################3
        # update storage price??? - tttt need to be considered on 10/27/2014
        ##########################################################################3

        return sfo_uid

    def upload_file_complete (self, clock, sfo_id, sfo_status, sfo_size):

        sfo = self.get_sfo_from_sc_file_objects (sfo_id, sfo_status, sfo_size)
        if sfo is None:
            print "[SC Obj Debug] Cannot find SFO (ID:%s, ST:%s, SZ:%s KB) from sc_file_objects" \
                  % (sfo_id, sfo_status, sfo_size)
            clib.sim_exit()


        ##########################################################################3
        # update storage price??? - tttt need to be considered on 10/27/2014
        ##########################################################################3

        return sfo.set_sfo_status (simgval.gSFO_ST_FILE_UPLOAD_COMPLETED, clock)

    def insert_sfo_into_sc_file_objects (self, sfo_obj):

        self.sc_cv.acquire()

        self.sc_file_objects.append(sfo_obj)

        self.sc_cv.notify()
        self.sc_cv.release()

    def get_sfo_from_sc_file_objects (self, sfo_id, sfo_status, sfo_size):

        ret_sfo = None

        self.sc_cv.acquire()
        for sfo in self.sc_file_objects:
            if sfo.sfo_id == sfo_id and sfo.sfo_status == sfo_status and sfo.sfo_size == sfo_size:
                ret_sfo = sfo
                break

        self.sc_cv.notify()
        self.sc_cv.release()

        return ret_sfo

    def increase_sc_volume (self, sfo_size):

        if sfo_size < 0:
            clib.sim_exit()

        self.sc_cv.acquire()

        self.sc_usage_volume += sfo_size

        self.sc_cv.notify()
        self.sc_cv.release()

    def get_total_size_of_sfos (self):

        total_size_sfos = 0

        for sfo in self.sc_file_objects:
            if sfo.sfo_status == simgval.gSFO_ST_FILE_UPLOAD_START or sfo.sfo_status == simgval.gSFO_ST_FILE_UPLOAD_COMPLETED:
                total_size_sfos += sfo.sfo_size

        return total_size_sfos

    def get_sc_duration (self):

        return self.sc_delete_time - self.sc_create_time

    def get_infos (self):

        return  "Storage Container <ID:" + str(self.sc_id) + ", CU:Job-" + str(self.sc_creator) + ", PR:$" + str(self.sc_usage_price) + ", SZ:" + str(self.sc_usage_volume/1024.0) + "MB, PM:" + str(simgval.get_sim_code_str(self.sc_permission)) + ", ST:" + str(simgval.get_sim_code_str(self.sc_status)) + ", CT:" + str(self.sc_create_time) + ", NSFO:" + str(len(self.sc_file_objects)) + ">"


    def display_storage_container_infos (self, clock):

        print ""
        print "[Comm.Lib] %d ======================================================================" % clock
        print "[Comm.Lib] %d Display Storage Container Obj Infos." % clock
        print "[Comm.Lib] %d ======================================================================" % clock
        print "[Comm.Lib] %d SC UID                  : %s" % (clock, self.sc_id)
        print "[Comm.Lib] %d SC Creator              : Job-%s" % (clock, self.sc_creator)
        print "[Comm.Lib] %d SC Terminator           : %s" % (clock, self.sc_terminator)
        print "[Comm.Lib] %d SC Region               : %s" % (clock, self.sc_region)
        print "[Comm.Lib] %d SC Permission           : %s" % (clock, simgval.get_sim_code_str(self.sc_permission))
        print "[Comm.Lib] %d SC Status               : %s" % (clock, simgval.get_sim_code_str(self.sc_status))
        print "[Comm.Lib] %d SC Create Time          : %s" % (clock, self.sc_create_time)
        print "[Comm.Lib] %d SC Delete Time          : %s" % (clock, self.sc_delete_time)
        print "[Comm.Lib] %d SC Usage Price          : $%s" % (clock, self.sc_usage_price)
        print "[Comm.Lib] %d SC Usage Volume         : %s KB (%s MB, %s GB, %s TB)" % (clock, self.sc_usage_volume, self.sc_usage_volume/1024.0, self.sc_usage_volume/(1024.0*1024), self.sc_usage_volume/(1024.0*1024*1024))
        print "[Comm.Lib] %d SC Num of File Objects  : %s SFOs" % (clock, len(self.sc_file_objects))

        if len(self.sc_file_objects) > 0:
            print "[Comm.Lib] %d ----------------------------------------------------------------------" % clock
            total_sfo_file_size = 0
            for sfo in self.sc_file_objects:
                print "[Comm.Lib] %d SC File Objects [%05d] : %s" % (clock, sfo.sfo_id, sfo.get_infos())
                total_sfo_file_size += sfo.sfo_size

            print "[Comm.Lib] %d SFO Total File Size     : %s KB (%s MB, %s GB, %s TB)" % (clock, total_sfo_file_size, total_sfo_file_size/1024.0, total_sfo_file_size/(1024.0*1024), self.sc_usage_volume/(1024.0*1024*1024))

        print "[Comm.Lib] %d ======================================================================" % clock

class Storage_File_Object:

    def __init__ (self, id, sc_id, act_size, owner, planned_size, clock):

        self.sfo_id = id
        self.sfo_sc_id = sc_id
        self.sfo_size = act_size            # KB
        self.sfo_owner = owner
        self.sfo_status = simgval.gSFO_ST_FILE_UPLOAD_START
        self.sfo_data_status = None             # Add this on 1027-2014 to handle cases when cloud storage doesn't have enough space for file.
        self.sfo_planned_size = planned_size    # Add this on 1027-2014 to handle cases when cloud storage doesn't have enough space for file.
        self.sfo_create_time = clock
        self.sfo_active_time = None
        self.sfo_delete_time = None
        self.sfo_usage_price = 0
        self.sfo_cv = Condition()

        if planned_size > act_size:
            # partial upload
            self.sfo_data_status = simgval.gSFO_DST_PARTIAL_UPLOAD

        elif planned_size == act_size:
            # full upload
            self.sfo_data_status = simgval.gSFO_DST_FULL_UPLOAD
        else:
            # error case
            print "[SFO Obj Debug] Error! - Actual Data Size (%s KB) exceed Planned Data Size (%s KB)" % (self.sfo_size, self.sfo_planned_size)
            clib.sim_exit()


    def set_sfo_status (self, update_status, clock):

        # status validation check #1
        if self.sfo_status == simgval.gSFO_ST_FILE_UPLOAD_START and update_status != simgval.gSFO_ST_FILE_UPLOAD_COMPLETED:
            return False

        # status validation check #2
        if self.sfo_status == simgval.gSFO_ST_FILE_UPLOAD_COMPLETED and update_status != simgval.gSFO_ST_FILE_DELETED:
            return False

        self.sfo_cv.acquire()
        self.sfo_status = update_status
        self.sfo_cv.notify()
        self.sfo_cv.release()

        if self.sfo_status == simgval.gSFO_ST_FILE_UPLOAD_COMPLETED:
            self.set_sfo_active_time (clock)

        elif self.sfo_status == simgval.gSFO_ST_FILE_DELETED:
            self.set_sfo_delete_time (clock)

        return True

    def set_sfo_active_time (self, clock):

        self.sfo_cv.acquire()
        self.sfo_active_time = clock
        self.sfo_cv.notify()
        self.sfo_cv.release()

    def set_sfo_delete_time (self, clock):

        self.sfo_cv.acquire()
        self.sfo_delete_time = clock
        self.sfo_cv.notify()
        self.sfo_cv.release()

    def set_sfo_usage_price (self, price):

        if self.sfo_usage_price > price:
            clib.sim_exit()

        self.sfo_cv.acquire()
        self.sfo_usage_price = price
        self.sfo_cv.notify()
        self.sfo_cv.release()

    def get_sfo_duration (self):

        return self.sfo_delete_time - self.sfo_active_time

    def get_infos (self):

        info_str = "<ID:%s, SC:%s, ON:Job-%s, SZ:%s KB, PR:$%s, ST:%s, DST:%s, PSZ:%s KB, CT:%s, AT:%s, DT:%s>" \
                  % (self.sfo_id, self.sfo_sc_id, self.sfo_owner, self.sfo_size, self.sfo_usage_price, simgval.get_sim_code_str(self.sfo_status), simgval.get_sim_code_str(self.sfo_data_status), self.sfo_planned_size, self.sfo_create_time, self.sfo_active_time, self.sfo_delete_time)
        return info_str

# UPDATE VM OBJECT
def generate_VM_UID ():

    global g_VM_UID
    global g_VM_UID_CV

    g_VM_UID_CV.acquire()
    g_VM_UID += 1
    val = g_VM_UID
    g_VM_UID_CV.notify()
    g_VM_UID_CV.release()

    return val

def set_VM_status (vm, status):

    if status != simgval.gVM_ST_CREATING and status != simgval.gVM_ST_ACTIVE and status != simgval.gVM_ST_TERMINATE:
        return

    if vm.status != status:
        g_VM_Obj_CV.acquire()
        vm.status = status
        g_VM_Obj_CV.notify()
        g_VM_Obj_CV.release()

def set_VM_time_active (vm, time_clock):

    if vm.time_active != time_clock:
        g_VM_Obj_CV.acquire()
        vm.time_active = time_clock
        g_VM_Obj_CV.notify()
        g_VM_Obj_CV.release()

def set_VM_time_terminate (vm, time_clock):

    if vm.time_terminate != time_clock:
        g_VM_Obj_CV.acquire()
        vm.time_terminate = time_clock
        g_VM_Obj_CV.notify()
        g_VM_Obj_CV.release()

def set_VM_startup_lag (vm, startup_lag):

    if vm.startup_lag != startup_lag:
        g_VM_Obj_CV.acquire()
        vm.startup_lag = startup_lag
        g_VM_Obj_CV.notify()
        g_VM_Obj_CV.release()

def set_VM_current_job (clock, vm, job):

    if job is not None: # update case
        if vm.current_job is not None:
            return False
    else:               # clear case
        if vm.current_job is None:
            return False

    g_VM_Obj_CV.acquire()
    vm.current_job = job
    g_VM_Obj_CV.notify()
    g_VM_Obj_CV.release()

    if job is None:
        set_VM_job_clock (vm, -1, clock)
        """
        this is legacy code -- 11/12/2014
        vm.current_job_start_clock = -1
        vm.last_job_complete_clock = clock
        """
    else:
        set_VM_job_clock (vm, clock, -1)
        """
        this is legacy code -- 11/12/2014
        vm.current_job_start_clock = clock
        vm.last_job_complete_clock = -1
        """
    return True

def set_VM_job_clock (vm, current_job_start_clock, last_job_complete_clock):

    g_VM_Obj_CV.acquire()
    vm.current_job_start_clock = current_job_start_clock
    vm.last_job_complete_clock = last_job_complete_clock
    g_VM_Obj_CV.notify()
    g_VM_Obj_CV.release()

def insert_job_into_VM_job_queue (vm, job):

    vm.job_queue_cv.acquire()
    vm.job_queue.append(job)
    vm.job_queue_cv.notify()
    vm.job_queue_cv.release()

def get_job_from_VM_job_queue (vm, job_id):

    if len(vm.job_queue) < 1:
        return None

    if vm.job_queue[0].id != job_id:
        return None

    vm.job_queue_cv.acquire()
    job = vm.job_queue.pop(0)
    vm.job_queue_cv.notify()
    vm.job_queue_cv.release()

    return job

def insert_job_into_VM_job_history (vm, job):

    vm.job_history_cv.acquire()
    vm.job_history.append(job)
    vm.job_history_cv.notify()
    vm.job_history_cv.release()

def get_vm_billing_clock (vm):

    if vm.status == simgval.gVM_ST_TERMINATE and vm.time_terminate is not None:
        return vm.time_terminate - vm.time_create
    else:
        return None

def set_VM_cost (vm, cost):

    if vm.cost != cost:
        g_VM_Obj_CV.acquire()
        vm.cost = cost
        g_VM_Obj_CV.notify()
        g_VM_Obj_CV.release()

def set_VM_sd_policy_activated_flag (vm, val):

    if vm.sd_policy_activated == val:
        return

    g_VM_Obj_CV.acquire()
    vm.sd_policy_activated = val
    g_VM_Obj_CV.notify()
    g_VM_Obj_CV.release()

    print ""
    print ""
    print "vm (ID:%d) sd_policy updated to %s" % (vm.id, vm.sd_policy_activated)
    print ""

    """
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)

    print ("\tCaller   : %s, %s:%d\n\n" % (calframe[1][3], calframe[1][1], calframe[1][2]))
    """


def set_VM_sd_wait_time (vm, val):

    if val < 0:
        return

    g_VM_Obj_CV.acquire()
    vm.sd_wait_time = val
    g_VM_Obj_CV.notify()
    g_VM_Obj_CV.release()

def set_VM_is_vscale_victim (vm, val):

    if val is True or val is False:

        g_VM_Obj_CV.acquire()
        vm.is_vscale_victim = val
        g_VM_Obj_CV.notify()
        g_VM_Obj_CV.release()

def set_VM_vscale_case (vm, val):

    if val is simgval.gVSO_VSCALE_UP or val is simgval.gVSO_VSCALE_DOWN:

        g_VM_Obj_CV.acquire()
        vm.vscale_case = val
        g_VM_Obj_CV.notify()
        g_VM_Obj_CV.release()

def set_VM_vscale_victim_id (vm, val):

    g_VM_Obj_CV.acquire()
    vm.vscale_victim_id = val
    g_VM_Obj_CV.notify()
    g_VM_Obj_CV.release()

# UPDATE JOB OBJECT

def generate_JOB_UID ():

    global g_JOB_UID
    global g_JOB_UID_CV

    g_JOB_UID_CV.acquire()
    g_JOB_UID += 1
    val = g_JOB_UID
    g_JOB_UID_CV.notify()
    g_JOB_UID_CV.release()


    return val

def set_Job_status (job, status):

    if status != simgval.gJOB_ST_GENERATED \
            and status != simgval.gJOB_ST_ASSIGNED \
            and status != simgval.gJOB_ST_STARTED \
            and status != simgval.gJOB_ST_COMPLETED \
            and status != simgval.gJOB_ST_FAILED:
        return

    if job.status != status:
        g_Job_Obj_CV.acquire()
        job.status = status
        g_Job_Obj_CV.notify()
        g_Job_Obj_CV.release()

def set_Job_time_assign (job, clock):

    if job.time_assign != clock:
        g_Job_Obj_CV.acquire()
        job.time_assign = clock
        g_Job_Obj_CV.notify()
        g_Job_Obj_CV.release()

def set_Job_time_start (job, clock):

    if job.time_start != clock:
        g_Job_Obj_CV.acquire()
        job.time_start = clock
        g_Job_Obj_CV.notify()
        g_Job_Obj_CV.release()

def set_Job_time_complete (job, clock):

    if job.time_complete != clock:
        g_Job_Obj_CV.acquire()
        job.time_complete = clock
        g_Job_Obj_CV.notify()
        g_Job_Obj_CV.release()

def set_Job_VM_info (job, vm_id, vm_type_name):

    g_Job_Obj_CV.acquire()
    job.VM_id = vm_id
    job.VM_type = vm_type_name
    g_Job_Obj_CV.notify()
    g_Job_Obj_CV.release()

def set_Job_act_duration_on_VM (job, job_durations, cpu_perf_factor):

    act_duration = job_durations[0]
    act_cputime = job_durations[1]
    act_nettime = job_durations[2]

    g_Job_Obj_CV.acquire()
    job.act_duration_on_VM = act_duration
    g_Job_Obj_CV.notify()
    g_Job_Obj_CV.release()

    # update cpu and net time
    set_Job_act_cputime_on_VM (job, act_cputime)
    set_Job_act_nettime_on_VM (job, act_nettime)

    # store exact point for job failure
    if job.std_job_failure_time > 0 and job.job_failure_recovered == False and cpu_perf_factor > 0:
        set_Job_vm_job_failure_time (job, int(math.ceil(job.std_job_failure_time * cpu_perf_factor)))

def adjust_Job_act_duration_on_VM (job):

    update_job_duration = job.act_cputime_on_VM + sum(job.act_nettime_on_VM)

    if update_job_duration != job.act_duration_on_VM:
        g_Job_Obj_CV.acquire()
        job.act_duration_on_VM = update_job_duration
        g_Job_Obj_CV.notify()
        g_Job_Obj_CV.release()

def set_Job_act_cputime_on_VM (job, act_cputime):

    g_Job_Obj_CV.acquire()
    job.act_cputime_on_VM = act_cputime
    g_Job_Obj_CV.notify()
    g_Job_Obj_CV.release()

def set_Job_act_nettime_on_VM (job, act_nettime):

    g_Job_Obj_CV.acquire()
    job.act_nettime_on_VM = act_nettime
    g_Job_Obj_CV.notify()
    g_Job_Obj_CV.release()

def set_Job_output_file_transfer_infos (job, transfer_status, transfer_size):

    if transfer_status == simgval.gSFO_DST_FULL_UPLOAD:
        if transfer_size != job.output_file_size:
            print "[DEBUG JOB OBJ] Output File Size (%s KB) and Transfer Size (%s KB) Mismatch! - transfer_status:%s" \
                  % (job.output_file_size, transfer_size, simgval.get_sim_code_str(transfer_status))
            clib.sim_exit()

    elif transfer_status == simgval.gSFO_DST_PARTIAL_UPLOAD:
        if transfer_status >= job.output_file_size:
            print "[DEBUG JOB OBJ] Transfer Size (%s KB) should be smaller than Output File Size (%s KB) transfer_status:%s" \
                  % (transfer_size, job.output_file_size, simgval.get_sim_code_str(transfer_status))
            clib.sim_exit()

    else:
        print "[DEBUG JOB OBJ] transfer_status Error! - %s" % (simgval.get_sim_code_str(transfer_status))
        clib.sim_exit()

    g_Job_Obj_CV.acquire()
    job.output_file_transfer_status = transfer_status
    job.output_file_transfer_size = transfer_size
    g_Job_Obj_CV.notify()
    g_Job_Obj_CV.release()

def set_Job_vm_job_failure_time (job, failure_time):

    g_Job_Obj_CV.acquire()
    job.vm_job_failure_time = failure_time
    g_Job_Obj_CV.notify()
    g_Job_Obj_CV.release()

def set_Job_job_failure_recovered (job, recover_flag):

    if job.job_failure_recovered != recover_flag:
        g_Job_Obj_CV.acquire()
        job.job_failure_recovered = recover_flag
        g_Job_Obj_CV.notify()
        g_Job_Obj_CV.release()


def get_job_info_str (job):
    if job is None:
        return "Job-" + str(None)
    else:
        job_info_str = "%s (ID:%s, ADR:%s [C:%s,I:%s/%s], ST:%s, SDR:%s)" % (job.name, job.id, job.act_duration_on_VM, job.act_cputime_on_VM, job.act_nettime_on_VM[0], job.act_nettime_on_VM[1], simgval.get_sim_code_str(job.status), job.std_duration)
        return job_info_str

# Related TO Config

def set_Config_sim_trace_interval (conf_obj, val):

    g_Config_Obj_CV.acquire()
    if val >= 60:
        conf_obj.sim_trace_interval = val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_g_debug_log (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.g_debug_log = val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_scale_down_policy (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.vm_scale_down_policy = simgval.get_sim_policy_code(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_log_path (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.log_path = val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_report_path (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.report_path = val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_scale_down_policy_unit (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.vm_scale_down_policy_unit = int(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_scale_down_policy_recent_sample_cnt (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.vm_scale_down_policy_recent_sample_cnt = int(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_scale_down_policy_param1 (conf_obj, val):
    g_Config_Obj_CV.acquire()
    conf_obj.vm_scale_down_policy_param1 = val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_scale_down_policy_param2 (conf_obj, val):
    g_Config_Obj_CV.acquire()
    conf_obj.vm_scale_down_policy_param2 = val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_scale_down_policy_param3 (conf_obj, val):
    g_Config_Obj_CV.acquire()
    conf_obj.vm_scale_down_policy_param3 = val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_scale_down_policy_min_wait_time (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.vm_scale_down_policy_min_wait_time = int(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_scale_down_policy_max_wait_time (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.vm_scale_down_policy_max_wait_time = simgval.get_sim_max_conf_val(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_workload_file (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.workload_file = "./workload/" + val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_workload_type (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.workload_type = simgval.get_sim_policy_code(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_job_assign_policy (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.job_assign_policy = simgval.get_sim_policy_code(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_job_assign_max_running_VMs (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.job_assign_max_running_vms = simgval.get_sim_max_conf_val(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_enable_vertical_scaling (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.enable_vertical_scaling = simgval.get_sim_vertical_scaling_config_val (val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vscale_operation (conf_obj, val):
    g_Config_Obj_CV.acquire()
    conf_obj.vscale_operation = simgval.get_sim_policy_code(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()


def set_Config_min_startup_lag (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.min_startup_lag = int (val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_max_startup_lag (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.max_startup_lag = int (val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_price (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.vm_unit_price = float (val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_config_file (conf_obj, file):

    g_Config_Obj_CV.acquire()
    conf_obj.vm_config_file_path = "./config/" + file
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_selection_method (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.vm_selection_method = simgval.get_sim_policy_code(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_no_of_vm_types (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.no_of_vm_types = int (val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_vm_types (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.vm_types = val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_billing_time_unit (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.billing_time_unit = simgval.get_sim_policy_code(val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_billing_time_period (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.billing_time_period = int (val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_billing_unit_clock (conf_obj):

    btu_clock = None

    if conf_obj.billing_time_unit == simgval.gBTU_HOUR:
        btu_clock = 3600

    elif conf_obj.billing_time_unit == simgval.gBTU_MIN:
        btu_clock = 60

    if btu_clock is None:
        return False

    g_Config_Obj_CV.acquire()
    conf_obj.billing_unit_clock = conf_obj.billing_time_period * btu_clock
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

    return True

def set_Config_vm_scale_down_policy_timer_unit (conf_obj):

    sdtu = None

    if conf_obj.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_HOUR:
        sdtu = 3600
    elif conf_obj.vm_scale_down_policy == simgval.gPOLICY_VM_SCALEDOWN_MIN:
        sdtu = 60
    else:
        return True

    if sdtu is None:
        return False

    g_Config_Obj_CV.acquire()
    conf_obj.vm_scale_down_policy_timer_unit = conf_obj.vm_scale_down_policy_unit * sdtu
    #conf_obj.vm_scale_down_policy_timer_unit = 100 # for test
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

    return True

def set_Config_max_capacity_of_storage (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.max_capacity_of_storage = int (val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_max_num_of_storage_containers (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.max_num_of_storage_containers = int (val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_storage_unit_cost (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.storage_unit_cost = val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_storage_billing_unit_clock (conf_obj, val):

    if val < 1:
        clib.sim_exit()

    g_Config_Obj_CV.acquire()
    conf_obj.storage_billing_unit_clock = val
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_perf_data_transfer (conf_obj, var_list):

    if len(var_list) !=3:
        clib.sim_exit()

    g_Config_Obj_CV.acquire()
    conf_obj.perf_data_transfer = var_list
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Config_cost_data_transfer (conf_obj, var_list):

    if len(var_list) != 3:
        clib.sim_exit()

    g_Config_Obj_CV.acquire()
    conf_obj.cost_data_transfer = var_list
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()

def set_Configure_prob_job_failure (conf_obj, val):

    prob = float (val)

    if prob > 0 and prob <= 1.0:
        g_Config_Obj_CV.acquire()
        conf_obj.prob_job_failure = prob
        g_Config_Obj_CV.notify()
        g_Config_Obj_CV.release()

def set_Configure_job_failure_policy (conf_obj, val):

    g_Config_Obj_CV.acquire()
    conf_obj.job_failure_policy = simgval.get_sim_policy_code (val)
    g_Config_Obj_CV.notify()
    g_Config_Obj_CV.release()


# UPDATE Storage Container Object
def generate_SC_UID ():

    global g_SC_UID
    global g_SC_UID_CV

    g_SC_UID_CV.acquire()
    g_SC_UID += 1
    val = g_SC_UID
    g_SC_UID_CV.notify()
    g_SC_UID_CV.release()

    return val

# Update Storage File Object
def generate_SFO_UID ():

    global g_SFO_UID
    global g_SFO_UID_CV

    g_SFO_UID_CV.acquire()
    g_SFO_UID += 1
    val = g_SFO_UID
    g_SFO_UID_CV.notify()
    g_SFO_UID_CV.release()

    return val
