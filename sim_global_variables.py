import sys
import sim_lib_common as clib

# Block ID
gBLOCK_START                                    = 0                                                 # 0
gBLOCK_MAIN                                     = gBLOCK_START + 1                                  # 1
gBLOCK_SIM_CLOCK                                = gBLOCK_MAIN + 1                                   # 2
gBLOCK_SIM_EVENT_HANDLER                        = gBLOCK_SIM_CLOCK + 1                              # 3
gBLOCK_SIM_ENTITY_START                         = gBLOCK_SIM_EVENT_HANDLER + 1                      # 4 - Sim Entity Start
gBLOCK_WORKLOAD_GEN                             = gBLOCK_SIM_ENTITY_START + 1                       # 5
gBLOCK_BROKER                                   = gBLOCK_WORKLOAD_GEN + 1                           # 6
gBLOCK_IAAS                                     = gBLOCK_BROKER + 1                                 # 7
gBLOCK_SIM_ENTITY_END                           = gBLOCK_IAAS + 1                                   # 8 - Sim Entity End
gBLOCK_END                                      = 1000                                              # 1000

# EVT_CODE (including BYPASS SUB EVENT)
gEVT_SIM_START                                  = gBLOCK_END + 1                                    # 1001 sim start event
gEVT_BYPASS                                     = gEVT_SIM_START + 1                                # 1002 sim bypass event (block1 <-- event_handler --> block 2)
gEVT_ACK                                        = gEVT_BYPASS + 1                                   # 1003 ack for general event (block --> event handler)
gEVT_SET_CLOCK                                  = gEVT_ACK + 1                                      # 1004 (activate event handler) set sim clock: clock --> event handler
gEVT_SET_CLOCK_ACK                              = gEVT_SET_CLOCK + 1                                # 1005 (activate clock block) ack for sim clock event : event handler --> clock
gEVT_SET_TIMER                                  = gEVT_SET_CLOCK_ACK + 1                            # 1006 register timer events (blocks --> event handler)
gEVT_EXP_TIMER                                  = gEVT_SET_TIMER + 1                                # 1007 timer expired (event handler --> blocks)
gEVT_WORK_GEN                                   = gEVT_EXP_TIMER + 1                                # 1008 job generate
gEVT_CREATE_VM                                  = gEVT_WORK_GEN + 1                                 # 1009 VM create
gEVT_NOTI_VM_ACTIVATED                          = gEVT_CREATE_VM + 1                                # 1010 VM ACTIVATED (IAAS --> BROKER)
gEVT_START_JOB                                  = gEVT_NOTI_VM_ACTIVATED + 1                        # 1011 Job Execution Start (Broker --> IAAS)
gEVT_JOB_COMPLETED_SUCCESS                      = gEVT_START_JOB + 1                                # 1012 Job Completed Success (IAAS --> Broker)
gEVT_JOB_COMPLETED_FAILURE                      = gEVT_JOB_COMPLETED_SUCCESS + 1                    # 1013 Job Completed Success (IAAS --> Broker)
gEVT_TERMINATE_VM                               = gEVT_JOB_COMPLETED_FAILURE + 1                    # 1014 VM terminate (Broker --> IaaS)
gEVT_NOTI_SIMENTIY_TERMINATED                   = gEVT_TERMINATE_VM + 1                             # 1015 Simulation Entity Terminated (WG/BR/IaaS --> Event Handler)
gEVT_PREPARE_SIMENTITY_TERMINATE                = gEVT_NOTI_SIMENTIY_TERMINATED + 1                 # 1016 Prepare Simulation Entity Termination (Event Handler --> WG/BR/IaaS)
gEVT_ASK_SIMENTITY_TERMINATION                  = gEVT_PREPARE_SIMENTITY_TERMINATE + 1              # 1017 [BLOCKS --> SEH] ASK SIM ENTITY TERMINATION
gEVT_CONFIRM_SIMENTITY_TERMINATION              = gEVT_ASK_SIMENTITY_TERMINATION + 1                # 1018 [SEH --> BLOCKS] CONFIRM SIM ENTITY TERMINATION
gEVT_REJECT_SIMENTITY_TERMINATION               = gEVT_CONFIRM_SIMENTITY_TERMINATION + 1            # 1019 [SEH --> BLOCKS] REJECT SIM ENTITY TERMINATION
gEVT_REQ_CLEAR_SD_TMR_EVENT                     = gEVT_REJECT_SIMENTITY_TERMINATION + 1             # 1020 [BROKER --> SEH] REQ TO CLEAR SD TIMER EVENT ONLY USED FOR JAT-bases SD Schemes such as SL/MINJAT/MAXJAT
gEVT_SUCCESS_CLEAR_SD_TMR_EVENT                 = gEVT_REQ_CLEAR_SD_TMR_EVENT + 1                   # 1021 [SEH --> BROKER] SUCCESS TO CLEAR SD TIMER EVENT ONLY USED FOR JAT-bases SD Schemes such as SL/MINJAT/MAXJAT
gEVT_DEBUG_DISPLAY_JOB_OBJ                      = gEVT_SUCCESS_CLEAR_SD_TMR_EVENT + 1               # 1022 [IAAS --> BROKER] Debug Event Display Job Obj
gEVT_JOB_DURATION_CHANGED                       = gEVT_DEBUG_DISPLAY_JOB_OBJ + 1                    # 1023 [IAAS --> BROKER] This event to notify job's duration has been changed in iaas level.
gEVT_RESTART_JOB                                = gEVT_JOB_DURATION_CHANGED + 1                     # 1024 [BROKER --> IAAS] Restart Failed Job
gEVT_SIM_END                                    = 2000

# EVT_SUB_CODE - Used for Timer Event
gEVT_SUB_WAKEUP                                 = gEVT_SIM_END + 1                                  # 2001 Event Sub - Timer Event - Wake Up
gEVT_SUB_SEND_JOB                               = gEVT_SUB_WAKEUP + 1                               # 2002 Event Sub - Timer Event - Job Send Event
gEVT_SUB_VM_STARTUP_COMPLETE                    = gEVT_SUB_SEND_JOB + 1                             # 2003 Event Sub - Timer Event - VM Startup Complete
gEVT_SUB_JOB_IN_CPU_OP_COMPLETE                 = gEVT_SUB_VM_STARTUP_COMPLETE + 1                  # 2004 Event Sub - Timer Event - JOB's INPUT + CPU OPERATION PROCESSING COMPLETE
                                                                                                    # This is a replacement of "gEVT_SUB_JOB_EXEC_COMPLETE (2004)"
gEVT_SUB_JOB_OUT_OP_COMPLETE                    = gEVT_SUB_JOB_IN_CPU_OP_COMPLETE + 1               # 2005 Event Sub - Timer Event - JOB's OUTPUT Processing Complete - added on 10/26/2014
gEVT_SUB_VM_SCALE_DOWN                          = gEVT_SUB_JOB_OUT_OP_COMPLETE + 1                  # 2006 Event Sub - Timer Event - VM Scale Down
gEVT_SUB_WORKGEN_TERMINATE                      = gEVT_SUB_VM_SCALE_DOWN + 1                        # 2007 Event Sub - Timer Event - Workload Gen Sim Entity Terminate
gEVT_SUB_PREPARE_TERMINATION                    = gEVT_SUB_WORKGEN_TERMINATE + 1                    # 2008 Event Sub - Timer Event - Prepare Sim Termination
gEVT_SUB_WRITE_SIM_TRACE_BROKER                 = gEVT_SUB_PREPARE_TERMINATION + 1                  # 2009 Event Sub - Timer Event - Write Trace (Broker)
gEVT_SUB_TERMINATE_VSCALE_VICTIM                = gEVT_SUB_WRITE_SIM_TRACE_BROKER + 1                      # 2010 Event Sub - Timer Event - Kill VScale Victim
gEVT_SUB_FILE_TRANSFER_COMPLETE_CLOUD_STORAGE   = gEVT_SUB_TERMINATE_VSCALE_VICTIM + 1              # 2011 Event Sub - Timer Event - File Transfer Complete to Cloud Storage
gEVT_SUB_WRITE_SIM_TRACE_IAAS                   = gEVT_SUB_FILE_TRANSFER_COMPLETE_CLOUD_STORAGE + 1 # 2012 Event Sub - Timer Event - Write Trace (IaaS)
gEVT_SUB_JOB_FAILED                             = gEVT_SUB_WRITE_SIM_TRACE_IAAS + 1                 # 2013 Event Sub - Timer Event - Job Failed (IaaS)
gEVT_SUB_END                                    = 3000                                              # 3000 Event Sub End

# JOB_STATUS : GENERATED -> ASSIGNED -> STARTED -> COMPLETED
gJOB_ST_GENERATED                               = gEVT_SUB_END + 1                                  # 3001 JOB GENERATED
gJOB_ST_ASSIGNED                                = gJOB_ST_GENERATED + 1                             # 3002 JOB ASSIGNED TO VM
gJOB_ST_STARTED                                 = gJOB_ST_ASSIGNED + 1                              # 3003 JOB STARTED
gJOB_ST_COMPLETED                               = gJOB_ST_STARTED + 1                               # 3004 JOB COMPLETED
gJOB_ST_FAILED                                  = gJOB_ST_COMPLETED + 1                             # 3005 JOB FAILED
gJOB_ST_END                                     = 3100                                              # 3100 JOB END CODE

# VM_STATUS : CREATING -> ACTIVE -> TERMINATE
gVM_ST_CREATING                                 = gJOB_ST_END + 1                                   # 3101 VM CREATING
gVM_ST_ACTIVE                                   = gVM_ST_CREATING + 1                               # 3102 VM ACTIVE
gVM_ST_TERMINATE                                = gVM_ST_ACTIVE + 1                                 # 3103 VM TERMINATE
gVM_ST_END                                      = 3200                                              # 3200 VM CODE END

# JOB ASSIGNMENT
gJOB_ASSIGN_TO_NEW_VM                           = gVM_ST_END + 1                                    # 3201 JOB ASSIGN TO NEW VM
gJOB_ASSIGN_TO_EXISTING_VM                      = gJOB_ASSIGN_TO_NEW_VM + 1                         # 3202 JOB ASSIGN TO EXISTING VM
gJOB_ASSIGN_ERROR                               = gJOB_ASSIGN_TO_EXISTING_VM + 1                    # 3203 JOB ASSIGN ERROR
gJOB_ASSIGN_CODE_END                            = 3300                                              # 3300 JOB ASSIGNMENT CODE END

# JOB_ASSIGN_POLICY
gPOLICY_JOBASSIGN_EDF                           = gJOB_ASSIGN_CODE_END                              # 3301 POLICY JOB ASSIGNMENT: EDF
gPOLICY_JOBASSIGN_END                           = 3400                                              # 3400 POLICY JOB ASSIGNMENT END

# VM SCALE DOWN POLICY
gPOLICY_VM_SCALEDOWN_IMMEDIATE                  = gPOLICY_JOBASSIGN_END + 1                         # 3401 POLICY VM SCALE DOWN: IMMEDIATE
gPOLICY_VM_SCALEDOWN_HOUR                       = gPOLICY_VM_SCALEDOWN_IMMEDIATE + 1                # 3402 POLICY VM SCALE DOWN: HOURLY (AWS)
gPOLICY_VM_SCALEDOWN_MIN                        = gPOLICY_VM_SCALEDOWN_HOUR + 1                     # 3403 POLICY VM SCALE DOWN: MIN (Azure/Google Compute Cloud)
gPOLICY_VM_SCALEDOWN_STARTUPLAG                 = gPOLICY_VM_SCALEDOWN_MIN + 1                      # 3404 POLICY VM SCALE DOWN: STARTUP LAG
gPOLICY_VM_SCALEDOWN_JAT_MEAN                   = gPOLICY_VM_SCALEDOWN_STARTUPLAG + 1               # 3405 POLICY VM SCALE DOWN: MEAN JOB ARRIVAL TIME
gPOLICY_VM_SCALEDOWN_JAT_MAX                    = gPOLICY_VM_SCALEDOWN_JAT_MEAN + 1                 # 3406 POLICY VM SCALE DOWN: MAX JOB ARRIVAL TIME
gPOLICY_VM_SCALEDOWN_JAT_MEAN_RECENT            = gPOLICY_VM_SCALEDOWN_JAT_MAX + 1                  # 3407 POLICY VM SCALE DOWN: MEAN RECENT N JOB ARRIVAL TIME
gPOLICY_VM_SCALEDOWN_JAT_MAX_RECENT             = gPOLICY_VM_SCALEDOWN_JAT_MEAN_RECENT + 1          # 3408 POLICY VM SCALE DOWN: MAX RECENT N JOB ARRIVAL TIME
gPOLICY_VM_SCALEDOWN_JAT_SLR                    = gPOLICY_VM_SCALEDOWN_JAT_MAX_RECENT + 1           # 3409 POLICY VM SCALE DOWN: Simple Linear Regression
gPOLICY_VM_SCALEDOWN_JAT_2PR                    = gPOLICY_VM_SCALEDOWN_JAT_SLR + 1                  # 3410 POLICY VM SCALE DOWN: Quadratic Linear Regreeion (2 Degree Polynomial Regression)
gPOLICY_VM_SCALEDOWN_JAT_3PR                    = gPOLICY_VM_SCALEDOWN_JAT_2PR + 1                  # 3411 POLICY VM SCALE DOWN: Cubic Linear Regreeion (3 Degree Polynomial Regression)
gPOLICY_VM_SCALEDOWN_JAT_LLR                    = gPOLICY_VM_SCALEDOWN_JAT_3PR + 1                  # 3412 POLICY VM SCALE DOWN: Local Linear Regression
gPOLICY_VM_SCALEDOWN_JAT_L2PR                   = gPOLICY_VM_SCALEDOWN_JAT_LLR + 1                  # 3413 POLICY VM SCALE DOWN: Local Quadratic (2 degree) Regression
gPOLICY_VM_SCALEDOWN_JAT_L3PR                   = gPOLICY_VM_SCALEDOWN_JAT_L2PR + 1                 # 3414 POLICY VM SCALE DOWN: Local Cubic (3 degree) Regression
gPOLICY_VM_SCALEDOWN_JAT_WMA                    = gPOLICY_VM_SCALEDOWN_JAT_L3PR + 1                 # 3415 POLICY VM SCALE DOWN: Weighted Moving Average
gPOLICY_VM_SCALEDOWN_JAT_ES                     = gPOLICY_VM_SCALEDOWN_JAT_WMA + 1                  # 3416 POLICY VM SCALE DOWN: Exponential Smoothing
gPOLICY_VM_SCALEDOWN_JAT_HWDES                  = gPOLICY_VM_SCALEDOWN_JAT_ES + 1                   # 3417 POLICY VM SCALE DOWN: Holt Winters Double Exponential Smoothing
gPOLICY_VM_SCALEDOWN_JAT_BRDES                  = gPOLICY_VM_SCALEDOWN_JAT_HWDES + 1                # 3418 POLICY VM SCALE DOWN: Brown Double Exponential Smoothing
gPOLICY_VM_SCALEDOWN_JAT_AR                     = gPOLICY_VM_SCALEDOWN_JAT_BRDES + 1                # 3419 POLICY VM SCALE DOWN: AR-Autoregressive
gPOLICY_VM_SCALEDOWN_JAT_ARMA                   = gPOLICY_VM_SCALEDOWN_JAT_AR + 1                   # 3420 POLICY VM SCALE DOWN: ARMA-Autoregressive and Moving Average
gPOLICY_VM_SCALEDOWN_JAT_ARIMA                  = gPOLICY_VM_SCALEDOWN_JAT_ARMA + 1                 # 3421 POLICY VM SCALE DOWN: ARIMA-Autoregressive Integrated Moving Average
gPOLICY_VM_SCALEDOWN_END                        = 3500                                              # 3500 POLICY VM SCALE DOWN END

# WORKLOAD TYPE
gWORKLOAD_TYPE_STEADY                           = gPOLICY_VM_SCALEDOWN_END + 1                      # 3501 STEADY WORKLOAD TYPE
gWORKLOAD_TYPE_BURSTY                           = gWORKLOAD_TYPE_STEADY + 1                         # 3501 BURSTY WORKLOAD TYPE
gWORKLOAD_TYPE_INCREMENTAL                      = gWORKLOAD_TYPE_BURSTY + 1                         # 3502 INCREMENTAL WORKLOAD TYPE
gWORKLOAD_TYPE_DECLINE                          = gWORKLOAD_TYPE_INCREMENTAL + 1                    # 3503 DECLINE WORKLOAD TYPE
gWORKLOAD_TYPE_RANDOM                           = gWORKLOAD_TYPE_DECLINE + 1                        # 3504 RABDOM WORKLOAD TYPE
gWORKLOAD_TYPE_END                              = 3600                                              # 3600 WORKLOAD TYPE END

# BILLING TIME UNIT
gBTU_HOUR                                       = gWORKLOAD_TYPE_END + 1                            # 3601 BILLING TIME UNIT: HOUR
gBTU_MIN                                        = gBTU_HOUR + 1                                     # 3602 BILLING TIME UNIT: MIN
gBTU_END                                        = 3700                                              # 3700 BILLING TIME UNIT END

# VM SELECTION METHOD
gVM_SEL_METHOD_COST                             = gBTU_END + 1                                      # 3701 VM SELECTION METHOD - COST-based
gVM_SEL_METHOD_PERF                             = gVM_SEL_METHOD_COST + 1                           # 3702 VM SELECTION METHOD - PERF-based
gVM_SEL_METHOD_COSTPERF                         = gVM_SEL_METHOD_PERF + 1                           # 3703 VM SELECTION METHOD - COST-PERF-based
gVM_SEL_METHOD_END                              = 3800                                              # 3800 VM SELECTION METHOD END

# Vertical Scaling Operation
gVSO_VSCALE_UP                                  = gVM_SEL_METHOD_END + 1                            # 3801 VSO_VSCALE_UP - VSCALE-UP Only
gVSO_VSCALE_DOWN                                = gVSO_VSCALE_UP + 1                                # 3802 VSO_VSCALE_DOWN - VSCALE-DOWN Only
gVSO_VSCALE_BOTH                                = gVSO_VSCALE_DOWN + 1                              # 3803 VSO_VSCALE_BOTH - VSCALE-UP and DOWN
gVSO_VSCALE_END                                 = 3900                                              # 3900 END OF VSCALE_Operation

# STORAGE CONTAINER STATUS
gSC_ST_CREATE                                   = gVSO_VSCALE_END + 1                               # 3901 Storage Container Status: Created
gSC_ST_DELETED                                  = gSC_ST_CREATE + 1                                 # 3902 Storage Container Status: Deleted
gSC_ST_END                                      = 4000                                              # 4000 End of Storage Container Status

# STORAGE CONTAINER PERMISSION
gSC_PERMISSION_PUBLIC                           = gSC_ST_END + 1                                    # 4001 Storage Container Permission: Public
gSC_PERMISSION_PRIVATE                          = gSC_PERMISSION_PUBLIC + 1                         # 4002 Storage Container Permission: Private
gSC_PERMISSION_GROUP                            = gSC_PERMISSION_PRIVATE + 1                        # 4003 Storage Container Permission: Public to Group
gSC_PERMISSION_END                              = 4100                                              # 4100 End of Storage Container Permission

# STORAGE_FILE_OBJECT STATUS
gSFO_ST_FILE_UPLOAD_START                       = gSC_PERMISSION_END + 1                            # 4101 Storage File Object Status: File Upload Start
gSFO_ST_FILE_UPLOAD_COMPLETED                   = gSFO_ST_FILE_UPLOAD_START + 1                     # 4102 Storage File Object Status: File Upload Completed
gSFO_ST_FILE_DELETED                            = gSFO_ST_FILE_UPLOAD_COMPLETED + 1                 # 4103 Storage File Object Status: File Deleted
gSFO_ST_END                                     = 4200                                              # 4200 End of Storage File Object Status

# For Workload Generator - File Flow Direction
gIOFTD_NONE                                     = gSFO_ST_END + 1                                   # 4201 No Input/Output file Used
gIFTD_IC                                        = gIOFTD_NONE + 1                                   # 4202 Input File is transferred from somewhere in Cloud
gIFTD_OC                                        = gIFTD_IC + 1                                      # 4203 Input File is transferred from outside of cloud
gOFTD_IC                                        = gIFTD_OC + 1                                      # 4204 Output file is transferred to somewhere in cloud
gOFTD_OC                                        = gOFTD_IC + 1                                      # 4205 Output file is transferred to Somewhere in outside of cloud
gIOFTD_END                                      = 4300

# Job File Info - Input/Output
gJOBFILE_INPUT                                  = gIOFTD_END + 1                                    # 4301 Job File - Input
gJOBFILE_OUTPUT                                 = gJOBFILE_INPUT + 1                                # 4302 Job File - Output
gJOBFILE_END                                    = 4400                                              # 4400 End of Job File

# SFO_DATA_STATUS
gSFO_DST_FULL_UPLOAD                            = gJOBFILE_END + 1                                  # 4401 Storage File Object Data Status : Full Upload
gSFO_DST_PARTIAL_UPLOAD                         = gSFO_DST_FULL_UPLOAD + 1                          # 4402 Storage File Object Data Status : Partial Upload
gSFO_DST_END                                    = 4500                                              # 4500 End of Storage File Object Data Status

# Job Failure Policy
gJFP1_IGNORE_JOB                                = gSFO_DST_END + 1                                  # 4501 Job Failure Policy #1 - ignore job
gJFP2_RE_EXECUTE_JOB                            = gJFP1_IGNORE_JOB + 1                              # 4502 Job Failure Policy #2 - reexecute the job
gJFP3_MOVE_JOB_TO_THE_END_OF_QUEUE              = gJFP2_RE_EXECUTE_JOB + 1                          # 4503 Job Failure Policy #3 - move job to the end of the queue
gJFP4_FIND_VM_EARLIEST_JOB_COMPLETION           = gJFP3_MOVE_JOB_TO_THE_END_OF_QUEUE + 1            # 4504 Job Failure Policy #4 - find a vm, which offers earliest job completion

def get_sim_code_str (code):
    if code is None:
        return "None"

    # BLOCK ID
    elif code == gBLOCK_MAIN:
        return "BLOCK_MAIN (" + str(code) + ")"
    elif code == gBLOCK_SIM_CLOCK:
        return "BLOCK_SIM_CLOCK (" + str(code) + ")"
    elif code == gBLOCK_SIM_EVENT_HANDLER:
        return "BLOCK_SIM_EVENT_HANDLER (" + str(code) + ")"
    elif code == gBLOCK_WORKLOAD_GEN:
        return "BLOCK_WORKLOAD_GEN (" + str(code) + ")"
    elif code == gBLOCK_BROKER:
        return "BLOCK_BROKER (" + str(code) + ")"
    elif code == gBLOCK_IAAS:
        return "BLOCK_IAAS (" + str(code) + ")"

    #EVT_CODE
    elif code == gEVT_SIM_START:
        return "EVT_SIM_START (" + str(code) + ")"
    elif code == gEVT_BYPASS:
        return "EVT_BYPASS (" + str(code) + ")"
    elif code == gEVT_ACK:
        return "EVT_ACK (" + str(code) + ")"
    elif code == gEVT_SET_CLOCK:
        return "EVT_SET_CLOCK (" + str(code) + ")"
    elif code == gEVT_SET_CLOCK_ACK:
        return "EVT_SET_CLOCK_ACK (" + str(code) + ")"
    elif code == gEVT_SET_TIMER:
        return "EVT_SET_TIMER (" + str(code) + ")"
    elif code == gEVT_EXP_TIMER:
        return "EVT_EXP_TIMER (" + str(code) + ")"
    elif code == gEVT_WORK_GEN:
        return "EVT_WORK_GEN (" + str(code) + ")"
    elif code == gEVT_CREATE_VM:
        return "EVT_CREATE_VM (" + str(code) + ")"
    elif code == gEVT_NOTI_VM_ACTIVATED:
        return "EVT_NOTI_VM_ACTIVATED (" + str(code) + ")"
    elif code == gEVT_START_JOB:
        return "EVT_START_JOB (" + str(code) + ")"
    elif code == gEVT_JOB_COMPLETED_SUCCESS:
        return "EVT_JOB_COMPLETED_SUCCESS (" + str(code) + ")"
    elif code == gEVT_JOB_COMPLETED_FAILURE:
        return "EVT_JOB_COMPLETED_FAILURE (" + str(code) + ")"
    elif code == gEVT_TERMINATE_VM:
        return "EVT_TERMINATE_VM (" + str(code) + ")"
    elif code == gEVT_NOTI_SIMENTIY_TERMINATED:
        return "EVT_NOTI_SIMENTIY_TERMINATED (" + str(code) + ")"
    elif code == gEVT_PREPARE_SIMENTITY_TERMINATE:
        return "(UNUSED EVT) EVT_PREPARE_SIMENTITY_TERMINATE (" + str(code) + ")"
    elif code == gEVT_ASK_SIMENTITY_TERMINATION:
        return "EVT_ASK_SIMENTITY_TERMINATION (" + str(code) + ")"
    elif code == gEVT_CONFIRM_SIMENTITY_TERMINATION:
        return "EVT_CONFIRM_SIMENTITY_TERMINATION (" + str(code) + ")"
    elif code == gEVT_REJECT_SIMENTITY_TERMINATION:
        return "EVT_REJECT_SIMENTITY_TERMINATION (" + str(code) + ")"
    elif code == gEVT_REQ_CLEAR_SD_TMR_EVENT:
        return "EVT_REQ_CLEAR_SD_TMR_EVENT (" + str(code) + ")"
    elif code == gEVT_SUCCESS_CLEAR_SD_TMR_EVENT:
        return "EVT_SUCCESS_CLEAR_SD_TMR_EVENT (" + str(code) + ")"
    elif code == gEVT_DEBUG_DISPLAY_JOB_OBJ:
        return "EVT_DEBUG_DISPLAY_JOB_OBJ (" + str(code) + ")"
    elif code == gEVT_JOB_DURATION_CHANGED:
        return "EVT_JOB_DURATION_CHANGED (" + str(code) + ")"
    elif code == gEVT_RESTART_JOB:
        return "EVT_RESTART_JOB (" + str(code) + ")"
    elif code == gEVT_SIM_END:
        return "EVT_SIM_END (" + str(code) + ")"

    #EVT_SUB_CODE
    elif code == gEVT_SUB_WAKEUP:
        return "EVT_SUB_WAKEUP (" + str(code) + ")"
    elif code == gEVT_SUB_SEND_JOB:
        return "EVT_SUB_SEND_JOB (" + str(code) + ")"
    elif code == gEVT_SUB_VM_STARTUP_COMPLETE:
        return "EVT_SUB_VM_STARTUP_COMPLETE (" + str(code) + ")"
    elif code == gEVT_SUB_JOB_IN_CPU_OP_COMPLETE:
        return "EVT_SUB_JOB_IN_CPU_OP_COMPLETE (" + str(code) + ")"
    elif code == gEVT_SUB_JOB_OUT_OP_COMPLETE:
        return "EVT_SUB_JOB_OUT_OP_COMPLETE (" + str(code) + ")"
    elif code == gEVT_SUB_VM_SCALE_DOWN:
        return "EVT_SUB_VM_SCALE_DOWN (" + str(code) + ")"
    elif code == gEVT_SUB_WORKGEN_TERMINATE:
        return "EVT_SUB_WORKGEN_TERMINATE (" + str(code) + ")"
    elif code == gEVT_SUB_PREPARE_TERMINATION:
        return "(UNUSED EVT) EVT_SUB_PREPARE_TERMINATION (" + str(code) + ")"
    elif code == gEVT_SUB_WRITE_SIM_TRACE_BROKER:
        return "gEVT_SUB_WRITE_SIM_TRACE (" + str(code) + ")"
    elif code == gEVT_SUB_TERMINATE_VSCALE_VICTIM:
        return "EVT_SUB_TERMINATE_VSCALE_VICTIM (" + str(code) + ")"
    elif code == gEVT_SUB_FILE_TRANSFER_COMPLETE_CLOUD_STORAGE:
        return "EVT_SUB_FILE_TRANSFER_COMPLETE_CLOUD_STORAGE (" + str(code) + ")"
    elif code == gEVT_SUB_WRITE_SIM_TRACE_IAAS:
        return "EVT_SUB_WRITE_SIM_TRACE_IAAS (" + str(code) + ")"
    elif code == gEVT_SUB_JOB_FAILED:
        return "EVT_SUB_JOB_FAILED (" + str(code) + ")"

    #JOB_STATUS:
    elif code == gJOB_ST_GENERATED:
        return "JOB_ST_GENERATED (" + str(code) + ")"
    elif code == gJOB_ST_ASSIGNED:
        return "JOB_ST_ASSIGNED (" + str(code) + ")"
    elif code == gJOB_ST_STARTED:
        return "JOB_ST_STARTED (" + str(code) + ")"
    elif code == gJOB_ST_COMPLETED:
        return "JOB_ST_COMPLETED (" + str(code) + ")"
    elif code == gJOB_ST_FAILED:
        return "JOB_ST_FAILED (" + str(code) + ")"
    elif code == gJOB_ST_END:
        return "JOB_ST_END (" + str(code) + ")"

    #VM_STATUS:
    elif code == gVM_ST_CREATING:
        return "VM_ST_CREATING (" + str(code) + ")"
    elif code == gVM_ST_ACTIVE:
        return "VM_ST_ACTIVE (" + str(code) + ")"
    elif code == gVM_ST_TERMINATE:
        return "VM_ST_TERMINATE (" + str(code) + ")"

    #JOB_ASSIGNMENT
    elif code == gJOB_ASSIGN_TO_NEW_VM:
        return "JOB_ASSIGN_TO_NEW_VM (" + str(code) + ")"
    elif code == gJOB_ASSIGN_TO_EXISTING_VM:
        return "JOB_ASSIGN_TO_EXISTING_VM (" + str(code) + ")"
    elif code == gJOB_ASSIGN_ERROR:
        return "JOB_ASSIGN_ERROR (" + str(code) + ")"

    # JOB_ASSIGN_POLICY
    elif code == gPOLICY_JOBASSIGN_EDF:
        return "POLICY_JOBASSIGN_EDF (" + str(code) + ")"

    # VM SCALE DOWN POLICY
    elif code == gPOLICY_VM_SCALEDOWN_IMMEDIATE:
        return "POLICY_VM_SCALEDOWN_IMMEDIATE (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_HOUR:
        return "POLICY_VM_SCALEDOWN_HOUR (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_MIN:
        return "POLICY_VM_SCALEDOWN_MIN (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_STARTUPLAG:
        return "POLICY_VM_SCALEDOWN_STARTUPLAG (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_MEAN:
        return "POLICY_VM_SCALEDOWN_JAT_MEAN (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_MAX:
        return "POLICY_VM_SCALEDOWN_JAT_MAX (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_MEAN_RECENT:
        return "POLICY_VM_SCALEDOWN_JAT_MEAN_RECENT (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_MAX_RECENT:
        return "POLICY_VM_SCALEDOWN_JAT_MAX_RECENT (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_SLR:
        return "POLICY_VM_SCALEDOWN_JAT_SLR (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_2PR:
        return "POLICY_VM_SCALEDOWN_JAT_2PR (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_3PR:
        return "POLICY_VM_SCALEDOWN_JAT_3PR (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_LLR:
        return "POLICY_VM_SCALEDOWN_JAT_LLR (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_L2PR:
        return "POLICY_VM_SCALEDOWN_JAT_L2PR (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_L3PR:
        return "POLICY_VM_SCALEDOWN_JAT_L3PR (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_WMA:
        return "POLICY_VM_SCALEDOWN_JAT_WMA (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_ES:
        return "POLICY_VM_SCALEDOWN_JAT_ES (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_HWDES:
        return "POLICY_VM_SCALEDOWN_JAT_HWDES (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_BRDES:
        return "POLICY_VM_SCALEDOWN_JAT_BRDES (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_AR:
        return "POLICY_VM_SCALEDOWN_JAT_AR (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_ARMA:
        return "POLICY_VM_SCALEDOWN_JAT_ARMA (" + str(code) + ")"
    elif code == gPOLICY_VM_SCALEDOWN_JAT_ARIMA:
        return "POLICY_VM_SCALEDOWN_JAT_ARIMA (" + str(code) + ")"


    # WORKLOAD TYPE
    elif code == gWORKLOAD_TYPE_STEADY:
        return "WORKLOAD_TYPE_STEADY (" + str(code) + ")"
    elif code == gWORKLOAD_TYPE_BURSTY:
        return "WORKLOAD_TYPE_BURSTY (" + str(code) + ")"
    elif code == gWORKLOAD_TYPE_INCREMENTAL:
        return "WORKLOAD_TYPE_INCREMENTAL (" + str(code) + ")"
    elif code == gWORKLOAD_TYPE_DECLINE:
        return "WORKLOAD_TYPE_DECLINE (" + str(code) + ")"
    elif code == gWORKLOAD_TYPE_RANDOM:
        return "WORKLOAD_TYPE_RANDOM (" + str(code) + ")"

    # BTU: Billing Time Unit
    elif code == gBTU_HOUR:
        return "BTU_HOUR (" + str(code) + ")"
    elif code == gBTU_MIN:
        return "BUT_MIN (" + str(code) + ")"

    # VM SELECTION METHOD
    elif code == gVM_SEL_METHOD_COST:
        return "VM_SEL_COST (" + str(code) + ")"
    elif code == gVM_SEL_METHOD_PERF:
        return "VM_SEL_PERF (" + str(code) + ")"
    elif code == gVM_SEL_METHOD_COSTPERF:
        return "VM_SEL_COSTPERF (" + str(code) + ")"

    # VSCALE CASE
    elif code == gVSO_VSCALE_UP:
        return "VSO_VSCALE_UP (" + str(code) + ")"
    elif code == gVSO_VSCALE_DOWN:
        return "VSO_VSCALE_DOWN (" + str(code) + ")"
    elif code == gVSO_VSCALE_BOTH:
        return "VSO_VSCALE_BOTH UP&DOWN  (" + str(code) + ")"

    # Storage Container Status
    elif code == gSC_ST_CREATE:
        return "SC_ST_CREATE (" + str(code) + ")"
    elif code == gSC_ST_DELETED:
        return "SC_ST_DELETED (" + str(code) + ")"

    # Storage Container Permission Code
    elif code == gSC_PERMISSION_PUBLIC:
        return "SC_PERMISSION_PUBLIC (" + str(code) + ")"
    elif code == gSC_PERMISSION_PRIVATE:
        return "SC_PERMISSION_PRIVATE (" + str(code) + ")"
    elif code == gSC_PERMISSION_GROUP:
        return "SC_PERMISSION_GROUP (" + str(code) + ")"


    # Storage File Object Status
    elif code == gSFO_ST_FILE_UPLOAD_START:
        return "SFO_ST_FILE_UPLOAD_START (" + str(code) + ")"
    elif code == gSFO_ST_FILE_UPLOAD_COMPLETED:
        return "SFO_ST_FILE_UPLOAD_COMPLETED (" + str(code) + ")"
    elif code == gSFO_ST_FILE_DELETED:
        return "SFO_ST_FILE_DELETED (" + str(code) + ")"


    # Input/Output File Flow Direction
    elif code == gIOFTD_NONE:
        return "IOFTD_NO (" + str(code) + ")"
    elif code == gIFTD_IC:
        return "IFTD_IC (" + str(code) + ")"
    elif code == gIFTD_OC:
        return "IFTD_OC (" + str(code) + ")"
    elif code == gOFTD_IC:
        return "OFTD_IC (" + str(code) + ")"
    elif code == gOFTD_OC:
        return "OFTD_OC (" + str(code) + ")"

    # Job File - Input/Output
    elif code == gJOBFILE_INPUT:
        return "JOBFILE_INPUT (" + str(code) + ")"
    elif code == gJOBFILE_OUTPUT:
        return "JOBFILE_OUTPUT (" + str(code) + ")"

    # SFO Data Status
    elif code == gSFO_DST_FULL_UPLOAD:
        return "SFO_DST_FULL_UPLOAD (" + str(code) + ")"
    elif code == gSFO_DST_PARTIAL_UPLOAD:
        return "SFO_DST_PARTIAL_UPLOAD (" + str(code) + ")"

    # Job Failure Recovery Policy
    elif code == gJFP1_IGNORE_JOB:
        return "JFP1_IGNORE_JOB (" + str(code) + ")"
    elif code == gJFP2_RE_EXECUTE_JOB:
        return "gJFP2_RE_EXECUTE_JOB (" + str(code) + ")"
    elif code == gJFP3_MOVE_JOB_TO_THE_END_OF_QUEUE:
        return "JFP3_MOVE_JOB_TO_THE_END_OF_QUEUE (" + str(code) + ")"
    elif code == gJFP4_FIND_VM_EARLIEST_JOB_COMPLETION:
        return "JFP4_FIND_VM_EARLIEST_JOB_COMPLETION (" + str(code) + ")"

    # Error Case
    else:
        return "SIM_CODE_ERROR! (" + str(code) + ")"

def get_sim_policy_code (str_code):

    # Policy #1: Job Assignment
    # EDF Job Scheduling
    if str_code == "EDF":
        return gPOLICY_JOBASSIGN_EDF

    # Policy #2: VM Scale Down
    # p1 Immediate Scale Down
    elif str_code == "SD-IM":
        return gPOLICY_VM_SCALEDOWN_IMMEDIATE

    # p2 HOURLY Scale Down
    elif str_code == "SD-HR":
        return gPOLICY_VM_SCALEDOWN_HOUR

    # p3 MIN Scale Down
    elif str_code == "SD-MN":
        return gPOLICY_VM_SCALEDOWN_MIN

    # p4 Startup Lag-based Scale Down
    elif str_code == "SD-SL":
        return gPOLICY_VM_SCALEDOWN_STARTUPLAG

    # p5 Mean Job Arrival Time Based Scale Down
    elif str_code == "SD-JAT-MEAN":
        return gPOLICY_VM_SCALEDOWN_JAT_MEAN

    # p6 MAX Job Arrival Time Based Scale Down
    elif str_code == "SD-JAT-MAX":
        return gPOLICY_VM_SCALEDOWN_JAT_MAX

    # p7 Mean Recent Job Arrival Time Based Scale Down
    elif str_code == "SD-JAT-MEAN-RECENT":
        return gPOLICY_VM_SCALEDOWN_JAT_MEAN_RECENT

    # p8 Max Recent Job Arrival Time Based Scale Down
    elif str_code == "SD-JAT-MAX-RECENT":
        return gPOLICY_VM_SCALEDOWN_JAT_MAX_RECENT

    # p9 Simple Linear Regression
    elif str_code == "SD-JAT-SLR":
        return gPOLICY_VM_SCALEDOWN_JAT_SLR

    # p10 Quadratic Regression (2 degree polynomial regression)
    elif str_code == "SD-JAT-2PR":
        return gPOLICY_VM_SCALEDOWN_JAT_2PR

    # p11 Cubic Regression (3 degree polynomial regression)
    elif str_code == "SD-JAT-3PR":
        return gPOLICY_VM_SCALEDOWN_JAT_3PR

    # p12 Local Linear Regression
    elif str_code == "SD-JAT-LLR":
        return gPOLICY_VM_SCALEDOWN_JAT_LLR

    # p13 Local Quadratic Regression
    elif str_code == "SD-JAT-L2PR":
        return gPOLICY_VM_SCALEDOWN_JAT_L2PR

    # p14 Local Cubic Regression
    elif str_code == "SD-JAT-L3PR":
        return gPOLICY_VM_SCALEDOWN_JAT_L3PR

    # p15 Weighted Moving Average
    elif str_code == "SD-JAT-WMA":
        return gPOLICY_VM_SCALEDOWN_JAT_WMA

    # p16 Exponential Smoothing
    elif str_code == "SD-JAT-ES":
        return gPOLICY_VM_SCALEDOWN_JAT_ES

    # p17 Holt Winters Double Exponential Smoothing
    elif str_code == "SD-JAT-HWDES":
        return gPOLICY_VM_SCALEDOWN_JAT_HWDES

    # p18 Brown Double Exponential Smoothing
    elif str_code == "SD-JAT-BRDES":
        return gPOLICY_VM_SCALEDOWN_JAT_BRDES

    # p19 Autoregressive
    elif str_code == "SD-JAT-AR":
        return gPOLICY_VM_SCALEDOWN_JAT_AR

    # p20 Autoregressive and Moving Average
    elif str_code == "SD-JAT-ARMA":
        return gPOLICY_VM_SCALEDOWN_JAT_ARMA

    # p21 Autoregressive Integrated Moving Average
    elif str_code == "SD-JAT-ARIMA":
        return gPOLICY_VM_SCALEDOWN_JAT_ARIMA

    # WORKLOAD TYPE
    elif str_code == "STEADY":
        return gWORKLOAD_TYPE_STEADY

    elif str_code == "BURSTY":
        return gWORKLOAD_TYPE_BURSTY

    elif str_code == "INCREMENTAL":
        return gWORKLOAD_TYPE_INCREMENTAL

    elif str_code == "DECLINE":
        return gWORKLOAD_TYPE_DECLINE

    elif str_code == "RANDOM":
        return gWORKLOAD_TYPE_RANDOM

    # BTU: Billing Time Unit
    elif str_code == "BTU_HOUR":
        return gBTU_HOUR

    elif str_code == "BTU_MIN":
        return gBTU_MIN

    # VM SELECTION METHOD
    elif str_code == "VM-SEL-COST":
        return gVM_SEL_METHOD_COST
    elif str_code == "VM-SEL-PERF":
        return gVM_SEL_METHOD_PERF
    elif str_code == "VM-SEL-COSTPERF":
        return gVM_SEL_METHOD_COSTPERF

    # VERTICAL SCALING
    elif str_code == "VSCALE-UP":
        return gVSO_VSCALE_UP
    elif str_code == "VSCALE-DOWN":
        return gVSO_VSCALE_DOWN
    elif str_code == "VSCALE-BOTH":
        return gVSO_VSCALE_BOTH

    # job failure recovery policy
    elif str_code == "JF-POLICY-01":
        return gJFP1_IGNORE_JOB
    elif str_code == "JF-POLICY-02":
        return gJFP2_RE_EXECUTE_JOB
    elif str_code == "JF-POLICY-03":
        return gJFP3_MOVE_JOB_TO_THE_END_OF_QUEUE
    elif str_code == "JF-POLICY-04":
        return gJFP4_FIND_VM_EARLIEST_JOB_COMPLETION

    return None

def get_sim_max_conf_val (val):

    # val is string
    if val == "UNLIMITED":
        return sys.maxint

    if int(val) < 1:
        return None
    else:
        return int(val)

def get_sim_vertical_scaling_config_val (val):

    if val == "YES":
        return True
    else:
        return False

def get_file_transfer_direction_config_val (direction_type, val):

    # if direction type == 1: input
    # if direction type == 2: output

    if direction_type != 1 and direction_type != 2:
        clib.sim_exit()

    if val != "NONE" and val != "IC" and val != "OC":
        clib.sim_exit()

    if val == "NONE":
        return gIOFTD_NONE

    # input file
    if direction_type == 1:

        if val == "IC":
            return gIFTD_IC
        elif val == "OC":
            return gIFTD_OC

    # output file
    else:

        if val == "IC":
            return gOFTD_IC
        elif val == "OC":
            return gOFTD_OC

    clib.sim_exit()

