import time
import os
import logging
import random
import sim_lib_common as clib
import sim_global_variables as simgval
from threading import Condition

Global_Curr_Sim_Clock_CV = Condition()
Global_Curr_Sim_Clock = -99

g_flag_sim_entity_term = False  # Sim Entity (Thread) Termination Flag
g_flag_sim_entity_term_CV = Condition()

g_Sim_Entity_Running_States = []
g_Sim_Entity_Running_States_CV = Condition()

g_log_handler = None
g_my_block_id = None

Timer_Events_CV = Condition()
Timer_Events = []

Sentout_Timer_Event_IDs_CV = Condition()
Sentout_Timer_Event_IDs = []

g_sim_conf  = None
gQ_SEH      = None
gQ_CLOCK    = None
gQ_WORK_GEN = None
gQ_BROKER   = None
gQ_IAAS     = None

def set_log (path):
    logger = logging.getLogger('sim_event_processing')
    log_file = path + '/[' + str(g_my_block_id) + ']-sim_event_processing.log'
    hdlr = logging.FileHandler(log_file)
    #hdlr = logging.FileHandler(path + '/sim_event_processing.log')
    formatter = logging.Formatter('%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

def init_sim_entity_running_state ():

    global g_Sim_Entity_Running_States

    g_Sim_Entity_Running_States_CV.acquire()

    for x in xrange(simgval.gBLOCK_SIM_ENTITY_END):
        if x <= simgval.gBLOCK_SIM_ENTITY_START:
            g_Sim_Entity_Running_States.append(False)
        else:
            g_Sim_Entity_Running_States.append(True)

    g_Sim_Entity_Running_States_CV.notify()
    g_Sim_Entity_Running_States_CV.release()

def set_SEH_sim_entity_term_flag (val):

    global g_flag_sim_entity_term
    if val is not True and val is not False:
        return

    if g_flag_sim_entity_term == val:
        return

    g_flag_sim_entity_term_CV.acquire()
    g_flag_sim_entity_term = val
    g_flag_sim_entity_term_CV.notify()
    g_flag_sim_entity_term_CV.release()

def set_sim_entity_running_state (block_id, state):

    global g_Sim_Entity_Running_States

    if g_Sim_Entity_Running_States[block_id] != state:
        g_Sim_Entity_Running_States_CV.acquire()
        g_Sim_Entity_Running_States[block_id] = state
        g_Sim_Entity_Running_States_CV.notify()
        g_Sim_Entity_Running_States_CV.release()

        print "[SimEvent] %s Set Sim Entity Running State:" % (str(Global_Curr_Sim_Clock))
        print "[SimEvent] %s %s state is changed to %s" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(block_id), str(g_Sim_Entity_Running_States[block_id]))


    else:
        print "[SimEvent] %s Set Sim Entity Running State Error! => Both Values are the same (OLD:%s, NEW:%s)!" \
              % (str(Global_Curr_Sim_Clock), g_Sim_Entity_Running_States[block_id], state)
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  Set Sim Entity Running State Error! => Both Values are the same (OLD:%s, NEW:%s)!'
                            % (g_Sim_Entity_Running_States[block_id], state))
        clib.sim_exit()

def get_sim_entity_ids_by_running_state (state):

    # state == True  : running state
    # state == False : stopped state

    if state is not True and state is not False:
        return None

    running_block_ids = []

    for x in xrange (simgval.gBLOCK_SIM_ENTITY_END - simgval.gBLOCK_SIM_ENTITY_START - 1):
        block_id = x + simgval.gBLOCK_SIM_ENTITY_START + 1
        if g_Sim_Entity_Running_States[block_id] is state:
            running_block_ids.append (block_id)

    return running_block_ids

def get_count_Sentout_Timer_Event_IDs ():
    global Sentout_Timer_Event_IDs
    return len (Sentout_Timer_Event_IDs)

def insert_Sentout_Timer_Event_IDs (evt_id):
    global Sentout_Timer_Event_IDs
    global Sentout_Timer_Event_IDs_CV

    Sentout_Timer_Event_IDs_CV.acquire()
    Sentout_Timer_Event_IDs.append(evt_id)
    Sentout_Timer_Event_IDs_CV.notify()
    Sentout_Timer_Event_IDs_CV.release()

def find_Sentout_Timer_Event_IDs (evt_id):
    global Sentout_Timer_Event_IDs

    try:
        index = Sentout_Timer_Event_IDs.index(evt_id)
        return index
    except:
        return -1

def remove_Sentout_Timer_Event_IDs (evt_id):
    global Sentout_Timer_Event_IDs
    global Sentout_Timer_Event_IDs_CV

    evt_index = find_Sentout_Timer_Event_IDs (evt_id)

    if evt_index < 0:
        print "[SimEvent] %s remove_Sentout_Timer_Event_IDs Error - Event ID not exist!!! (evt_id:%s)" % (Global_Curr_Sim_Clock, evt_id)
        return False
    else:
        sentout_evt = Sentout_Timer_Event_IDs [evt_index]
        if sentout_evt != evt_id:
            print "[SimEvent] %s remove_Sentout_Timer_Event_IDs Error - Event ID mismatch!!! (list:%s, evt_id:%s)" % (Global_Curr_Sim_Clock, sentout_evt, evt_id)
            return False
        else:
            Sentout_Timer_Event_IDs_CV.acquire()
            Sentout_Timer_Event_IDs.pop(evt_index)
            Sentout_Timer_Event_IDs_CV.notify()
            Sentout_Timer_Event_IDs_CV.release()
            return True

def display_Timer_Events_List ():

    print "[SimEvent] %s Display Timer Event List " % (str(Global_Curr_Sim_Clock))
    print "[SimEvent] %s ================================================================" % (str(Global_Curr_Sim_Clock))

    for seq, te in enumerate (Timer_Events):

        tmr     = te[0]
        s_blk   = simgval.get_sim_code_str(te[1][0])
        s_ec    = simgval.get_sim_code_str(te[1][1])
        s_ed    = str(te[1][2])

        print "[SimEvent] %s [%d] Tmr: %d, BLK: %s, EC: %s, ED: %s" \
              % (str(Global_Curr_Sim_Clock), seq, tmr, s_blk, s_ec, s_ed)

    print "[SimEvent] %s ================================================================" % (str(Global_Curr_Sim_Clock))

def logwrite_Timer_Events_List (log_handler, title):

    g_log_handler.info(str(Global_Curr_Sim_Clock) + "  %s " % title)
    g_log_handler.info(str(Global_Curr_Sim_Clock) + "  ================================================================")

    for seq, te in enumerate (Timer_Events):

        tmr     = te[0]
        s_blk   = simgval.get_sim_code_str(te[1][0])
        s_ec    = simgval.get_sim_code_str(te[1][1])
        s_ed    = str(te[1][2])

        g_log_handler.info(str(Global_Curr_Sim_Clock) + "  [%d] Tmr: %d, BLK: %s, EC: %s, ED: %s"
                            % (seq, tmr, s_blk, s_ec, s_ed))

    g_log_handler.info(str(Global_Curr_Sim_Clock) + "  ================================================================")

def insert_Timer_Events(evt):
    global Timer_Events
    global Timer_Events_CV

    Timer_Events_CV.acquire()
    Timer_Events.append(evt)

    # sort list
    Timer_Events.sort (key=lambda e: e[0])

    Timer_Events_CV.notify()
    Timer_Events_CV.release()

def update_Timer_Events (time_value):
    global Timer_Events
    global Timer_Events_CV

    Timer_Events_CV.acquire()
    Timer_Events = [[e[0]+time_value, e[1]] for e in Timer_Events]
    Timer_Events_CV.notify()
    Timer_Events_CV.release()

def remove_Timer_Event (no_of_evts):

    global Timer_Events
    global Timer_Events_CV

    Timer_Events_CV.acquire()

    evt_index = 0
    no_of_del_evts = 0
    for x in xrange(no_of_evts):
        if Timer_Events[evt_index][0] < 1:
            Timer_Events.pop(0)
            no_of_del_evts += 1
        else:
            evt_index += 1

    Timer_Events_CV.notify()
    Timer_Events_CV.release()

    return no_of_del_evts

def find_Timer_Event_by_block (block_code):

    for timer_event in Timer_Events:
        if timer_event[1][0] == block_code and timer_event[1][1] != simgval.gEVT_SUB_WRITE_SIM_TRACE_BROKER and timer_event[1][1] != simgval.gEVT_SUB_WRITE_SIM_TRACE_IAAS:
            return True

    return False

def remove_Timer_Event_by_Index (index):

    global Timer_Events
    global Timer_Events_CV

    try:

        Timer_Events_CV.acquire()
        Timer_Events.pop(index)
        Timer_Events_CV.notify()
        Timer_Events_CV.release()

    except:

        print "[SimEvent] %s remove_Timer_Event_by_Index Error!" % (str(Global_Curr_Sim_Clock))
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  remove_Timer_Event_by_Index Error!')
        clib.sim_exit()

def get_index_Timer_Event_by_Infos (block_code, evt_code, evt_data1):

    # this should be used for clear scale down timer event
    if evt_code != simgval.gEVT_SUB_VM_SCALE_DOWN:
        return -2, None

    for te in Timer_Events:

        blk = te[1][0]
        ec  = te[1][1]
        ed  = te[1][2]

        if blk == block_code and ec == evt_code and ed[0] == evt_data1:
            return Timer_Events.index(te), te


    return -1, None

def get_evt_timeout_Timer_Events (index):
    global Timer_Events

    try:
        return Timer_Events[index]
    except:
        return None

def get_indice_timeout_Timer_Events ():
    global Timer_Events
    indice = []

    i = 0
    for Timer_Event in Timer_Events:
        if Timer_Event[0] < 1:
            indice.append(i)
        i += 1

    return indice

def get_number_of_Timer_Events():

    return len(Timer_Events)

# calculate the number of timer events except for gEVT_SUB_WRITE_SIM_TRACE_BROKER (2007) and gEVT_SUB_WRITE_SIM_TRACE_IAAS (2012)
def get_number_of_imp_Timer_Events ():

    no_of_evts = 0
    for tmr_evt in Timer_Events:
        if tmr_evt[1][1] != simgval.gEVT_SUB_WRITE_SIM_TRACE_BROKER and tmr_evt[1][1] != simgval.gEVT_SUB_WRITE_SIM_TRACE_IAAS:
            no_of_evts += 1

    return no_of_evts

def get_first_tick_Timer_Event ():

    if get_number_of_Timer_Events() > 0:
        return Timer_Events[0][0]
    else:
        return 1

def send_EVT_to_Block (evt_code, dst_block_id, evt):

    ret_val     = False
    flag_insert = True

    # BY PASS EVENT
    if evt_code == simgval.gEVT_BYPASS:
        if dst_block_id == simgval.gBLOCK_BROKER:
            gQ_BROKER.put (evt)
            ret_val = True
        elif dst_block_id == simgval.gBLOCK_IAAS:
            gQ_IAAS.put (evt)
            ret_val = True
        elif dst_block_id == simgval.gBLOCK_SIM_EVENT_HANDLER:
            gQ_SEH.put (evt)
            ret_val = True

    # TIMER EXPIRE EVENT
    elif evt_code == simgval.gEVT_EXP_TIMER:
        if dst_block_id == simgval.gBLOCK_WORKLOAD_GEN:
            gQ_WORK_GEN.put(evt)
            ret_val = True
        elif dst_block_id == simgval.gBLOCK_IAAS:
            gQ_IAAS.put(evt)
            ret_val = True
        elif dst_block_id == simgval.gBLOCK_BROKER:
            gQ_BROKER.put (evt)
            ret_val = True
        elif dst_block_id == simgval.gBLOCK_SIM_EVENT_HANDLER:
            gQ_SEH.put (evt)
            ret_val = True

    # EVT - SIM ENTITY TERMINATION
    elif evt_code == simgval.gEVT_CONFIRM_SIMENTITY_TERMINATION or evt_code == simgval.gEVT_REJECT_SIMENTITY_TERMINATION:
        if dst_block_id == simgval.gBLOCK_IAAS:
            gQ_IAAS.put(evt)
            ret_val = True
        elif dst_block_id == simgval.gBLOCK_BROKER:
            gQ_BROKER.put (evt)
            ret_val = True

    # SPECIAL EVT - gEVT_SUCCESS_CLEAR_SD_TMR_EVENT
    elif evt_code == simgval.gEVT_SUCCESS_CLEAR_SD_TMR_EVENT:
        if dst_block_id == simgval.gBLOCK_BROKER:
            gQ_BROKER.put (evt)
            ret_val = True

    # EVT_SEND TO SEH (MYSELF)
    # [1] TIMER REGISTER EVENT or [2] ACK EVT
    elif evt_code == simgval.gEVT_SET_TIMER or evt_code == simgval.gEVT_ACK:
        if dst_block_id == simgval.gBLOCK_SIM_EVENT_HANDLER:
            gQ_SEH.put (evt)
            ret_val = True
            flag_insert = False

    # for debug
    else:
        print "[SimEvent] %s send_EVT_to_Block Error! - Invalid EVT_CODE (%s) => " \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), evt
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  send_EVT_to_Block Error! - Invalid EVT_CODE (%s) => %s'
                       % (simgval.get_sim_code_str(evt_code), str(evt)))

        clib.sim_exit()

    # Insert Evt ID into Sentout Timer Event if both ret_val (q_put ok) and flag_insert are True
    if ret_val is True and flag_insert is True:
        insert_Sentout_Timer_Event_IDs (evt[0])

    return ret_val

def SEH_evt_set_clock_processing (q_msg):

    evt_clock       = q_msg[1]  # EVT_CLOCK
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE : Timer Ticks

    return_vals = [False, None]

    if evt_code != simgval.gEVT_SET_CLOCK:
        print "[SimEvent] %s SEH_evt_set_clock_processing EVT_CODE (%s) Error! => " \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  SEH_evt_set_clock_processing EVT_CODE ERROR! - %s' % str(q_msg))

        clib.sim_exit()
        return return_vals

    #print "EVT CODE:%s EVT_DATA:%d" % (evt_code, evt_data)
    if Global_Curr_Sim_Clock == evt_clock:
        return return_vals

    update_curr_sim_clock (evt_clock)
    print "[SimEvent] %s Update Sim Clock" % (str(Global_Curr_Sim_Clock))

    if Global_Curr_Sim_Clock > 0:
        ticks = evt_sub_code

        g_log_handler.info (ticks)
        update_Timer_Events (-ticks)

    evt_indice = get_indice_timeout_Timer_Events()
    if len(evt_indice) > 0:

        print "[SimEvent] %s IN  Timer_Events=>" % str(Global_Curr_Sim_Clock), Timer_Events
        print "[SimEvent] %s IN  Sentout_Timer_Event_IDs=>" % str(Global_Curr_Sim_Clock), Sentout_Timer_Event_IDs
        print "[SimEvent] %s %d Timer_Event(s) need to send out" % (str(Global_Curr_Sim_Clock), len(evt_indice))

        no_q_out = 0
        for evt_index in evt_indice:
            so_evt = get_evt_timeout_Timer_Events(evt_index)
            if so_evt is not None:
                so_evt_timeout = so_evt[0]
                so_evt_data = so_evt[1]

                if so_evt_timeout > 0:
                    print "[SimEvent] %s Timer is Malfunctioning!!! - timeout:%d" % (str(Global_Curr_Sim_Clock), so_evt_timeout)
                    g_log_handler.error(str(Global_Curr_Sim_Clock) + '  Timer is Malfunctioning!!! - timeout:%d'
                                  % so_evt_timeout)
                    clib.sim_exit()
                else:
                    #[DEST_BLOCK, EVT_CODE, EVT_DATA[]]
                    #[simgval.gBLOCK_WORKLOAD_GEN, "EVT_WAKEUP", None]
                    dst_block_id = so_evt_data[0]
                    q_evt_so = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, dst_block_id, simgval.gEVT_EXP_TIMER, so_evt_data[1], so_evt_data[2])

                    r = send_EVT_to_Block(simgval.gEVT_EXP_TIMER, dst_block_id, q_evt_so)
                    if r is True:
                        no_q_out += 1
                        # q_evt_id = q_evt_so[0]
                        # insert_Sentout_Timer_Event_IDs (q_evt_id) --> merged with send_EVT_to_Block 08072014
                    else:
                        print "[SimEvent] %s Timer Event Send Out Error!!! - Invalid Dest Block %s" \
                              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(so_evt_data[0]))
                        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  Timer Event Send Out Error!!! - Invalid Dest Block - %s'
                                  % simgval.get_sim_code_str(so_evt_data[0]))
                        clib.sim_exit()

            else:
                print "[SimEvent] %s No Item in Timer_Event[%d]" % (str(Global_Curr_Sim_Clock), evt_index)

        no_deleted_evts = remove_Timer_Event(len(evt_indice))
        print "[SimEvent] %s %d Timer_Event(s) actually sent out" % (str(Global_Curr_Sim_Clock), no_q_out)
        print "[SimEvent] %s %d Timer_Event(s) are removed from Timer_Events" % (str(Global_Curr_Sim_Clock), no_deleted_evts)
        print "[SimEvent] %s OUT Timer_Events=>" % str(Global_Curr_Sim_Clock), Timer_Events
        print "[SimEvent] %s OUT Sentout_Timer_Event_IDs=>" % str(Global_Curr_Sim_Clock), Sentout_Timer_Event_IDs

    else:
        print "[SimEvent] %s Current Timer_Events => %s" % (str(Global_Curr_Sim_Clock), str(Timer_Events))
        print "[SimEvent] %s No Timer Event for this time clock => Activate Sim_Clock" % (str(Global_Curr_Sim_Clock))

        ticks = get_first_tick_Timer_Event()
        return_vals = [True, ticks]

    return return_vals

def SEH_evt_set_timer_processing (q_msg):

    evt_code  = q_msg[4]  # EVT_CODE
    if evt_code != simgval.gEVT_SET_TIMER:
        print "[SimEvent] %s SEH_evt_set_timer_processing EVT_CODE (%s) Error! => " \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  SEH_evt_set_timer_processing EVT_CODE ERROR! - %s'
                            % str(q_msg))

        clib.sim_exit()

    g_log_handler.info(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Processing! - %s'
                            % (simgval.get_sim_code_str(evt_code), str(q_msg)))

    evt_clock       = q_msg[1]  # EVT_CLOCK
    evt_src         = q_msg[2]  # EVT_SRC
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE
    evt_data        = q_msg[6]  # EVT_DATA

    # Regist Timer Event
    # [DEST_BLOCK, EVT_SUB_CODE, EVT_DATA[]]
    # [simgval.gBLOCK_WORKLOAD_GEN, "EVT_WAKEUP", None]

    timer_evt = []
    timer_evt.append(evt_clock)

    timer_evt_obj = []
    timer_evt_obj.append(evt_src)
    timer_evt_obj.append(evt_sub_code)
    timer_evt_obj.append(evt_data)

    timer_evt.append(timer_evt_obj)

    if evt_sub_code == simgval.gEVT_SUB_VM_SCALE_DOWN:
        logwrite_Timer_Events_List (g_log_handler, "BEFORE " + simgval.get_sim_code_str(evt_code) + " RECV FOR VM-" + str(evt_data[0]) + ", SET CLOCK:" + str(evt_data[1]) + ", TIMER:" + str(evt_clock))

    insert_Timer_Events(timer_evt)

    if evt_sub_code == simgval.gEVT_SUB_VM_SCALE_DOWN:
        logwrite_Timer_Events_List (g_log_handler, "AFTER " + simgval.get_sim_code_str(evt_code) + " RECV FOR VM-" + str(evt_data[0]) + ", SET CLOCK:" + str(evt_data[1]) + ", TIMER:" + str(evt_clock))

    print "[SimEvent] %s RECV   EVT_SET_TIMER => %s" % (str(Global_Curr_Sim_Clock), str(q_msg))
    print "[SimEvent] %s INSERT Timer Event   => %s" % (str(Global_Curr_Sim_Clock), str(Timer_Events))

    g_log_handler.info(str(Global_Curr_Sim_Clock) + '  RECV   EVT_SET_TIMER => %s' % str(q_msg))
    g_log_handler.info(str(Global_Curr_Sim_Clock) + '  INSERT Timer Event   => %s' % str(Timer_Events))

def SEH_evt_ack_processing (q_msg):

    evt_id      = q_msg[0]  # EVT_ID
    evt_code    = q_msg[4]  # EVT_CODE
    return_vals  = [False, None]

    if evt_code != simgval.gEVT_ACK:
        print "[SimEvent] %s SEH_evt_ack_processing EVT_CODE (%s) Error! => " % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  SEH_evt_ack_processing EVT_CODE ERROR! - %s' % str(q_msg))

        clib.sim_exit()
        return return_vals

    print "[SimEvent] %s RECV EVT_ACK => %s" % (str(Global_Curr_Sim_Clock), str(q_msg))
    print "[SimEvent] %s IN  Sentout_Timer_Event_IDs => %s" % (str(Global_Curr_Sim_Clock), str(Sentout_Timer_Event_IDs))

    print evt_id

    r = remove_Sentout_Timer_Event_IDs (evt_id)
    if r is False:
        clib.sim_exit()

    print "[SimEvent] %s remove_Sentout_Timer_Event_IDs [%d]" % (str(Global_Curr_Sim_Clock), evt_id)
    print "[SimEvent] %s OUT Sentout_Timer_Event_IDs => %s" % (str(Global_Curr_Sim_Clock), str(Sentout_Timer_Event_IDs))

    no_so_evt = get_count_Sentout_Timer_Event_IDs()
    if no_so_evt == 0:
        return_vals = [True, 1]
        print "[SimEvent] %s No Sentout_Timer_Event_IDs => Activate Sim_Clock" % (str(Global_Curr_Sim_Clock))

        '''
        print "\n\n[SimEvent] SEH_evt_ack_processing = Timer Event =>"
        for e in Timer_Events:
            print e
        clib.sim_long_sleep()
        clib.sim_long_sleep()
        # activate timer thread
        '''
    return return_vals

def SEH_evt_bypass_processing (q_msg):

    evt_src         = q_msg[2]  # EVT_SRC
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE
    evt_data        = q_msg[6]  # EVT_DATA

    if evt_code != simgval.gEVT_BYPASS:
        print "[SimEvent] %s SEH_evt_bypass_processing EVT_CODE (%s) Error! => " % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  SEH_evt_bypass_processing EVT_CODE ERROR! - %s' % str(q_msg))
        clib.sim_exit()
    else:
        g_log_handler.info(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Processing - %s' % (simgval.get_sim_code_str(evt_code), str(q_msg)))

    print "[SimEvent] %s IN  Sentout_Timer_Event_IDs=>" % str(Global_Curr_Sim_Clock), Sentout_Timer_Event_IDs

    so_evt_dst_block = evt_sub_code
    so_evt_code = evt_data.pop(0)   # EVT_DATA[0] -- Actual Event Code.
    so_evt_sub_code = evt_src
    so_evt_data = evt_data

    q_evt_so = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, so_evt_dst_block, so_evt_code, so_evt_sub_code, so_evt_data)
    r = send_EVT_to_Block (evt_code, so_evt_dst_block, q_evt_so)
    if r is False:
        print "[SimEvent] %s EVT_BYPASS DST BLOCK (%s) ERROR!!!" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(so_evt_dst_block))
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_BYPASS DST BLOCK (%s) ERROR!!!' % (simgval.get_sim_code_str(so_evt_dst_block)))
        clib.sim_exit()
        return

    #q_evt_id = q_evt_so[0]
    #insert_Sentout_Timer_Event_IDs (q_evt_id) ==> Merged With send_EVT_to_Block 08072014

    print "[SimEvent] %s 1 BYPASS_EVT:%s sent out" % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(so_evt_code))
    print "[SimEvent] %s Out Sentout_Timer_Event_IDs=>" % str(Global_Curr_Sim_Clock), Sentout_Timer_Event_IDs

def SEH_evt_noti_simentity_terminated (q_msg):

    evt_src         = q_msg[2]  # EVT_SRC
    evt_code        = q_msg[4]  # EVT_CODE

    if evt_code != simgval.gEVT_NOTI_SIMENTIY_TERMINATED:
        print "[SimEvent] %s EVT_CODE (%s) Error! => " % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) ERROR! - %s' % (simgval.get_sim_code_str(evt_code), str(q_msg)))
        clib.sim_exit()

    remaining_events_exist = find_Timer_Event_by_block (evt_src)
    if remaining_events_exist is True:
        print "[SimEvent] %s EVT_CODE (%s) Processing Error! => Timer Events for %s Exist!" \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_src))
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Processing ERROR! - Timer Events for %s Exist!'
                            % (simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_src)))
        clib.sim_exit()

    #print g_Sim_Entity_Running_States[simgval.gBLOCK_SIM_ENTITY_START+1:]
    set_sim_entity_running_state (evt_src, False)
    print "[SimEvent] %s EVT_CODE (%s) Processing => %s" \
          % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), str(g_Sim_Entity_Running_States[simgval.gBLOCK_SIM_ENTITY_START+1:]))

    # [condition 1] if all sim entities are false (stopped), send termination noti event to sim_clock
    all_entities_terminated = all (state is False for state in g_Sim_Entity_Running_States [simgval.gBLOCK_SIM_ENTITY_START+1:])

    # [condition 2] no timer events
    no_of_tmr_evts = get_number_of_imp_Timer_Events ()

    if all_entities_terminated is True and no_of_tmr_evts < 1:

        # Set Termination Flag
        set_SEH_sim_entity_term_flag (True)

        # simulation complete event
        sim_end_evt = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, simgval.gBLOCK_SIM_CLOCK, simgval.gEVT_SIM_END, None, None)
        gQ_CLOCK.put(sim_end_evt)


def SEH_evt_ask_simentity_termination (q_msg):

    evt_src  = q_msg[2]  # EVT_SRC
    evt_code = q_msg[4]  # EVT_CODE
    baseline_block_id = simgval.gBLOCK_WORKLOAD_GEN

    if evt_code != simgval.gEVT_ASK_SIMENTITY_TERMINATION:
        print "[SimEvent] %s EVT_CODE (%s) Error! => " % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) ERROR! - %s' % (simgval.get_sim_code_str(evt_code), str(q_msg)))
        clib.sim_exit()

    if evt_src == baseline_block_id:
        print "[SimEvent] %s EVT_CODE (%s) Error! - Invalid SRC Block (%s) =>" \
                 % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_src)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Error! - Invalid SRC Block (%s) => %s'
                            % (simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_src), str(q_msg)))
        clib.sim_exit()

    print "[SimEvent] %s IN  Sentout_Timer_Event_IDs => %s" % (str(Global_Curr_Sim_Clock), str(Sentout_Timer_Event_IDs))

    term_cfm_evt_code = None
    # Workload Gen Sim Entity is Running
    if g_Sim_Entity_Running_States[baseline_block_id] is True:

        # cannot be terminated

        print "[SimEvent] %s EVT_CODE (%s) Processing - %s CANNOT be terminated! - %s [%s] is running!" \
                 % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_src), simgval.get_sim_code_str(baseline_block_id), str(g_Sim_Entity_Running_States[baseline_block_id]))
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Processing - %s CANNOT be terminated! - %s [%s] is running!'
                            % (simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_src), simgval.get_sim_code_str(baseline_block_id), str(g_Sim_Entity_Running_States[baseline_block_id])))

        # Send EVT_REJECT_SIMENTITY_TERMINATION to SRC BLOCK
        term_cfm_evt_code = simgval.gEVT_REJECT_SIMENTITY_TERMINATION

    # Workload Gen Block is not Running
    else:

        no_of_tmr_evts = get_number_of_imp_Timer_Events ()
        if no_of_tmr_evts < 1:

            # termination confirm - no timer events and baseline block already terminated.

            # Send gEVT_CONFIRM_SIMENTITY_TERMINATION to SRC BLOCK
            print "[SimEvent] %s EVT_CODE (%s) Processing - %s CAN be terminated! - %s [%s] is stopped and %d Timer Event(s) remain(s)!" \
                     % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_src), simgval.get_sim_code_str(baseline_block_id), str(g_Sim_Entity_Running_States[baseline_block_id]), no_of_tmr_evts)
            g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Processing - %s CAN be terminated! - %s [%s] is stopped and %d Timer Event(s) remain(s)!'
                                % (simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_src), simgval.get_sim_code_str(baseline_block_id), str(g_Sim_Entity_Running_States[baseline_block_id]), no_of_tmr_evts))

            term_cfm_evt_code = simgval.gEVT_CONFIRM_SIMENTITY_TERMINATION
        else:

            """
            clib.sim_clearscreen()
            print Timer_Events
            clib.sim_exit()
            """

            # cannot be terminated --> timer events exits
            print "[SimEvent] %s EVT_CODE (%s) Processing - %s CANNOT be terminated! - %d Timer Event(s) remain(s)" \
                 % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_src), no_of_tmr_evts)
            g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Processing - %s CANNOT be terminated! - %d Timer Event(s) remain(s)'
                            % (simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_src), no_of_tmr_evts))

            # Send EVT_REJECT_SIMENTITY_TERMINATION to SRC BLOCK
            term_cfm_evt_code = simgval.gEVT_REJECT_SIMENTITY_TERMINATION

    dst_block_id = evt_src
    term_cfm_evt = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, dst_block_id, term_cfm_evt_code, None, None)
    r = send_EVT_to_Block(term_cfm_evt_code, dst_block_id, term_cfm_evt)
    if r is not True:
        print "[SimEvent] %s Timer Event Send Out Error!!! - %s," \
              % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(term_cfm_evt_code)), term_cfm_evt
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  Timer Event Send Out Error!!! - %s, %s'
                  % (simgval.get_sim_code_str(term_cfm_evt_code), str(term_cfm_evt)))
        clib.sim_exit()

    print "[SimEvent] %s OUT Sentout_Timer_Event_IDs => %s" % (str(Global_Curr_Sim_Clock), str(Sentout_Timer_Event_IDs))

def SEH_evt_clear_sd_tmr_event (q_msg):

    evt_id          = q_msg[0]  # EVT_ID
    evt_src         = q_msg[2]  # EVT_SRC
    evt_code        = q_msg[4]  # EVT_CODE
    evt_real_src    = q_msg[5]  # EVT_SUB_CODE
    evt_data        = q_msg[6]  # EVT_DATA

    if evt_code != simgval.gEVT_REQ_CLEAR_SD_TMR_EVENT:
        print "[SimEvent] %s EVT_CODE (%s) Error! => " % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) ERROR! - %s' % (simgval.get_sim_code_str(evt_code), str(q_msg)))
        clib.sim_exit()

    if evt_real_src != simgval.gBLOCK_BROKER:
        print "[SimEvent] %s EVT_CODE (%s) Error! - Invalid REAL SRC Block (%s) =>" \
                 % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_real_src)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Error! - Invalid REAL SRC Block (%s) => %s'
                            % (simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_real_src), str(q_msg)))
        clib.sim_exit()

    vm_obj = evt_data[0]
    exception_flag = evt_data[1]

    if vm_obj.sd_policy_activated != True:
        print "[SimEvent] %s EVT_CODE (%s) Error! - Invalid Access to Lib:SEH_evt_clear_sd_tmr_event =>" \
                 % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Error! - Invalid Access to Lib:SEH_evt_clear_sd_tmr_event => %s'
                            % (simgval.get_sim_code_str(evt_code), str(q_msg)))
        clib.logwrite_vm_obj(g_log_handler, Global_Curr_Sim_Clock, vm_obj)
        clib.sim_exit()

    #display_Timer_Events_List ()
    logwrite_Timer_Events_List(g_log_handler, "BEFORE " + simgval.get_sim_code_str(evt_code) + " RECV FOR VM-" + str(vm_obj.id))
    clib.logwrite_vm_obj(g_log_handler, Global_Curr_Sim_Clock, vm_obj)

    index, te = get_index_Timer_Event_by_Infos (simgval.gBLOCK_BROKER, simgval.gEVT_SUB_VM_SCALE_DOWN, vm_obj.id)
    if index < 0 or te is None:

        # this case hardly happens but it exists -- no just warning instead of terminating simulator - 10/27/2014
        print "[SimEvent] %s EVT_CODE (%s) Error! - Proper Timer Event for VM (ID:%d) Scale Down not found!!!" \
                 % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), vm_obj.id)
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Error! - Proper Timer Event for VM(ID:%d) Scale Down not found!!!'
                            % (simgval.get_sim_code_str(evt_code), vm_obj.id))
        #clib.sim_exit()



    else:

        # proper event exists

        te_blk  = te[1][0]
        te_ec   = te[1][1]
        te_data = te[1][2]

        if  te_blk      != simgval.gBLOCK_BROKER or \
            te_ec       != simgval.gEVT_SUB_VM_SCALE_DOWN or \
            te_data[0]  != vm_obj.id or \
            index       <  0:

            print "[SimEvent] %s EVT_CODE (%s) Error! - Timer Event (IDX:%d, EC:%s) for VM(ID:%d) Scale Down Mismatch!" \
                     % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code),
                        index, simgval.get_sim_code_str(te_ec),vm_obj.id)
            g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) Error! - Timer Event (IDX:%d, EC:%s) for VM(ID:%d) Scale Down Mismatch!'
                                % (simgval.get_sim_code_str(evt_code), index, simgval.get_sim_code_str(te_ec),vm_obj.id))
            clib.sim_exit()


        remove_Timer_Event_by_Index (index)
        print "[SimEvent] %s EVT_CODE (%s) - Remove Timer Event (IDX:%d, EC:%s) for VM(ID:%d) Scale Down" \
                 % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), index, simgval.get_sim_code_str(te_ec),vm_obj.id)
        g_log_handler.info(str(Global_Curr_Sim_Clock) + '  EVT_CODE (%s) - Remove Timer Event (IDX:%d, EC:%s) for VM(ID:%d) Scale Down'
                           % (simgval.get_sim_code_str(evt_code), index, simgval.get_sim_code_str(te_ec),vm_obj.id))

        #display_Timer_Events_List ()
        logwrite_Timer_Events_List(g_log_handler, "AFTER " + simgval.get_sim_code_str(evt_code) + " RECV FOR VM-" + str(vm_obj.id))

        # send by pass to notify "request to clear sd event completed"
        seh_dst_block_id    = simgval.gBLOCK_BROKER
        seh_evt_code        = simgval.gEVT_SUCCESS_CLEAR_SD_TMR_EVENT
        seh_evt_id_data     = [vm_obj, exception_flag]

        seh_evt = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, seh_dst_block_id, seh_evt_code, None, seh_evt_id_data)
        ret = send_EVT_to_Block (seh_evt_code, seh_dst_block_id, seh_evt)

    # send ack
    SEH_send_evt_ack (evt_id)

# ACK SEND SEH -> SEH
def SEH_send_evt_ack (evt_id):

    dst_block_id = simgval.gBLOCK_SIM_EVENT_HANDLER
    evt_code = simgval.gEVT_ACK

    ack_evt = clib.make_queue_event (0, g_my_block_id, dst_block_id, evt_code, None, None)
    ack_evt[0] = evt_id
    send_EVT_to_Block(evt_code, dst_block_id, ack_evt)

    return

def SEH_event_processing (q_msg):

    evt_src         = q_msg[2]  # EVT_SRC
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE
    evt_data        = q_msg[6]  # EVT_DATA

    print "\n\n[SimEvent] %s EVT_RECV : EC:%s, ESC:%s, SRC_BLOCK:%s, data:" \
          % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), simgval.get_sim_code_str(evt_src)), evt_data
    g_log_handler.info(str(Global_Curr_Sim_Clock) + '  EVT_RECV - EC:%s, ESC: %s, SRC_BLOCK:%s, data: %s'
             % (simgval.get_sim_code_str(evt_code), simgval.get_sim_code_str(evt_sub_code), simgval.get_sim_code_str(evt_src), str(evt_data)))

    print_bar()

    ret_vals = [False, None]
    if evt_code == simgval.gEVT_SET_CLOCK:

        # SET CLOCK Event Processing
        # ret_val is related to increase sim clock
        ret_vals = SEH_evt_set_clock_processing (q_msg)
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  SEH_evt_set_clock_processing returns = %s' %(str(ret_vals)))

    elif evt_code == simgval.gEVT_SET_TIMER:

        # SET TIMER Event Processing
        SEH_evt_set_timer_processing (q_msg)

    elif evt_code == simgval.gEVT_ACK:

        # EVT ACK PROCESSING
        ret_vals = SEH_evt_ack_processing (q_msg)

        print ret_vals

        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  SEH_evt_ack_processing returns = %s' %(str(ret_vals)))

    elif evt_code == simgval.gEVT_BYPASS:

        # EVT BYPASS PROCESSING
        SEH_evt_bypass_processing (q_msg)

    elif evt_code == simgval.gEVT_NOTI_SIMENTIY_TERMINATED:

        # SIM ENTITY TERMINATION NOTIFICATION
        SEH_evt_noti_simentity_terminated (q_msg)

    elif evt_code == simgval.gEVT_ASK_SIMENTITY_TERMINATION:

        # CHECK SIM ENTITY CAN BE TERMINATED
        SEH_evt_ask_simentity_termination (q_msg)

    elif evt_code == simgval.gEVT_REQ_CLEAR_SD_TMR_EVENT:

        # CHECK SIM ENTITY CAN BE TERMINATED
        SEH_evt_clear_sd_tmr_event (q_msg)

    else:
        print "[SimEvent] %s EVT_CODE (%s) Error! => " % (str(Global_Curr_Sim_Clock), simgval.get_sim_code_str(evt_code)), q_msg
        g_log_handler.error(str(Global_Curr_Sim_Clock) + '  EVT_CODE ERROR! - %s' % str(q_msg))
        clib.sim_exit()

    return ret_vals

def SEH_term_event_processing (q_msg):

    evt_id          = q_msg[0]  # EVT_ID
    evt_src         = q_msg[2]  # EVT_SRC
    evt_code        = q_msg[4]  # EVT_CODE
    evt_sub_code    = q_msg[5]  # EVT_SUB_CODE
    evt_data        = q_msg[6]  # EVT_DATA

    if g_flag_sim_entity_term is not True:
        return False

    if evt_src == simgval.gBLOCK_SIM_CLOCK and evt_code == simgval.gEVT_SIM_END:
        return True
    else:
        return False

def print_bar ():
    print "-----------------------------------------------------------------------"

def wakeup_sim_clock (flag_send_ack, jump_clock):
    if flag_send_ack is not True:
        return

    # send ack packet to clock - active clock thread
    q_evt_clock = clib.make_queue_event (Global_Curr_Sim_Clock, g_my_block_id, simgval.gBLOCK_SIM_CLOCK, simgval.gEVT_SET_CLOCK_ACK, jump_clock, None)
    gQ_CLOCK.put(q_evt_clock)

def update_curr_sim_clock (clock):
    global Global_Curr_Sim_Clock
    global Global_Curr_Sim_Clock_CV

    if Global_Curr_Sim_Clock == clock:
        return

    #update_curr_sim_clock
    Global_Curr_Sim_Clock_CV.acquire()
    Global_Curr_Sim_Clock = clock
    Global_Curr_Sim_Clock_CV.notify()
    Global_Curr_Sim_Clock_CV.release()

    g_log_handler.info(str(Global_Curr_Sim_Clock) + '  Update Global_Curr_Sim_Clock: %s' % str(Global_Curr_Sim_Clock))

def th_sim_event (my_block_id, conf_obj, q_in, q_clock, q_work_gen, q_broker, q_IaaS):

    global Global_Curr_Sim_Clock
    global g_log_handler
    global g_my_block_id
    global Timer_Events # just for debug
    global g_sim_conf

    # global queue
    global gQ_SEH
    global gQ_CLOCK
    global gQ_WORK_GEN
    global gQ_BROKER
    global gQ_IAAS

    if my_block_id != simgval.gBLOCK_SIM_EVENT_HANDLER:
        print "[SimEvent] SimEvent Block ID (I:%d, G:%d) Error!!!" % (my_block_id, simgval.gBLOCK_SIM_EVENT_HANDLER)
        clib.sim_exit()

    g_my_block_id = my_block_id
    g_sim_conf = conf_obj
    gQ_SEH      = q_in
    gQ_CLOCK    = q_clock
    gQ_WORK_GEN = q_work_gen
    gQ_BROKER   = q_broker
    gQ_IAAS     = q_IaaS

    init_sim_entity_running_state()
    time.sleep(random.random())

    print "[SimEvent] Start Sim Event Thread!"
    log = set_log (g_sim_conf.log_path)
    g_log_handler = log
    log.info('0' + '  Sim Event Processing Log Start...')

    # Timer_Event = [timeout, [DEST_BLOCK, EVT_SUB_CODE, EVT_DATA[]]]
    # if EVT_DATA [] is None -> create an evt before send.
    init_event = [1, [simgval.gBLOCK_WORKLOAD_GEN, simgval.gEVT_SUB_WAKEUP, None]]
    insert_Timer_Events (init_event)

    while True:
        q_message = q_in.get()
        q_in.task_done()

        # ------------------------
        # EVENT MESSAGE
        # ------------------------
        # EVENT_SRC = Event Message Sender
        # EVENT_DST = Event Message Receiver
        # EVENT_CODE = Event Code
        # EVENT_SUB_CODE = Event Sub Code
        # EVENT_DATA = Event DaTa
        # ------------------------

        evt_dst = q_message[3]

        if evt_dst != my_block_id:
            print "[SimEvent] EVT_MSG ERROR! (Wrong EVT_DST) - ", q_message
            log.error(str(Global_Curr_Sim_Clock) + '  EVT_MSG ERROR! (Wrong EVT_DST) - %s' % str(q_message))
            clib.sim_exit()
            continue

        # Termination Processing
        if g_flag_sim_entity_term is True:
            wanna_term = SEH_term_event_processing (q_message)
            if wanna_term is True:
                break

        # SEH : Sim Event Handler
        flag_send_ack, jump_clock = SEH_event_processing (q_message)

        # activate timer thread
        wakeup_sim_clock (flag_send_ack, jump_clock)

        print_bar ()

    print "[SimEvent] %s Sim Event Thread Terminated." % (Global_Curr_Sim_Clock + 1)
