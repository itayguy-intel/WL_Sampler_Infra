# Run command from host using communicator
import os
import time
import signal
from  multiprocess import Process
from datetime import datetime
import pickle as pkl
import json
import sys
import pandas as pd
import argparse
import pprint
from evtar.services.communicator.ux import Communicator, CommunicatorConfig



# def ctrl_c_binding():
    # def handler(signum, frame):
        # res = input("Ctrl-c was pressed. Do you really want to exit? y/n ")
        # if res == 'y':
            # exit(1)

    # signal.signal(signal.SIGINT, handler)


def thermapy_func_wrapper(ip, duration, output_path, app_flow_path, resolution, launch_path):
    import __main__
    import sys
    import time
    sys.path.append(app_flow_path)
    if "cpu" not in __main__.__dict__.keys():

        from alderlake import startadl_adp
        startadl_adp.main()

        cpu = __main__.cpu
    else:
        cpu = __main__.cpu
    # with open(launch_path, 'w+') as f:
        # pass
    print('$$ Finish launch pythonsv$$')
    import application_collection as appf
    time_func = lambda: time.time() * resolution
    print('Finish improt appf')
    # cannot receive lambda function from outside...
    appf.collect_application_dts_time_freq(ip=ip, duration=duration, output_path=output_path, time_func=time_func)

        
    
def init_common_time(communicator_obj, resolution, time_func):
    t1 = time_func()
    target_time = communicator_obj.ExecuteCommandOnTarget(f'echo %date%-%time%', logOutput=False)
    t2 = time_func()
    # Calculate how long does it take to receive answer from the target using communicator
    penalty_time = (t2 - t1) / 2
    target_time = target_time.split('\r\n')[0]
    formatted_time = datetime.strptime(target_time, "%a %m/%d/%Y-%H:%M:%S.%f")
    base_epoch_time = (formatted_time - datetime(1970, 1, 1)).total_seconds() * resolution + penalty_time
    return base_epoch_time, t2
    
    
def enable_emon(emon_cmd_params, emon_target_output_path):
    command = '\"{setup_cmd}\" && {emon_cmd} -l{l} -t{t} -C \"{C}\" -f {f} -V'.format(**emon_cmd_params, f=emon_target_output_path)
    print(command)
    command_pid = Communicator.ExecuteCommandOnTargetAsync(command, bOrphan=False)
    print(f"Emon PID: {command_pid}")
    return command_pid
    

def enable_thermapy(ip, raw_data_path, data_collection_duration, launching_duration, lab_path, resolution, launch_path):
    thermapy_app_flow_path = os.path.join(lab_path, r'flows\application')
    thermapy_process = Process(target=thermapy_func_wrapper, kwargs={'ip': ip, 'duration': data_collection_duration, 'output_path': raw_data_path, 'app_flow_path': thermapy_app_flow_path, 'resolution': args.resolution, 'launch_path': launch_path})
    thermapy_process.start()
    print(f'Thermapy PID: ', thermapy_process.pid)
    time.sleep(launching_duration)
    return thermapy_process
    

def thermapy_post_processing(lab_path, raw_data_path, parsed_output_file):
    sys.path.append(lab_path)
    from thermapy_app_parser.thermapy_data_parse import ThermapyDataParser 
    parser = ThermapyDataParser()
    parser.parse_file(input_file=raw_data_path, output_file=parsed_output_file)
    parsed_df = pd.read_csv(parsed_output_file)
    parsed_df.to_csv(parsed_output_file, index=False)
    return parsed_df

# daq align in function
# time.sleep(1)
    # try:
        # Communicator.ExecuteCommandOnTarget(command=align_cmd, sCommandCwd=align_dir)
    # except:
        # pass
    # time.sleep(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='.')
    parser.add_argument('--cfg_path', type=str, required=False, default=r'C:\SVSHARE\WL_Sampler_Infra\wl_sampler_config.json', help='.')
    parser.add_argument('--resolution', type=int, required=False, default=1000, help='.')
    args = parser.parse_args()


    time_func = lambda: time.time() * args.resolution
    # read json config
    with open(args.cfg_path) as f:
        cfg = json.load(f)
        
    pp = pprint.PrettyPrinter(indent=4)
    print(f"Input Configurations from: {args.cfg_path}")
    print(pp.pprint(cfg))

    # Extracts configurations from static file
    emon_output_filename = cfg.get('emon_output_filename')
    emon_cmd_params = cfg.get('emon_cmd_params')
    target_dir = cfg.get('target_dir')
    host_dir = cfg.get('host_dir')
    # Create output dir in host
    if not os.path.isdir(host_dir):
        os.mkdir(host_dir)
    
    
    thermapy_launch_path = os.path.join(host_dir, 'thermpy_launch.txt')
    ip = cfg.get('thermapy_ip_target')
    data_collection_duration = None
    #data_collection_duration = cfg.get('thermapy_data_collection_duration') * args.resolution
    thermapy_launching_duration = cfg.get('thermapy_launching_duration')
    thermapy_lab_path = cfg.get('thermapy_lab_code_path')
    thermapy_output_filename = cfg.get('thermapy_output_filename')
    wl_dir = cfg.get('wl_dir')
    wl_cmd = cfg.get('wl_cmd')
    wl_duration = cfg.get('wl_duration')
    align_dir = cfg.get('alignment_exe_dir')
    align_cmd = cfg.get('alignment_exe_cmd')
    
    CommunicatorConfig.Target.IsConnectedTimeoutSec = cfg.get('Target.IsConnectedTimeoutSec')
    CommunicatorConfig.Target.DefaultPeer2PeerIP = cfg.get('Target.DefaultPeer2PeerIP')
    print(f"Is Target Connected: {Communicator.IsConnected()}")
    
    # Create output dir in target
    Communicator.ExecuteCommandOnTarget(command=f'mkdir {target_dir}')
    
    base_epoch_time, t2 = init_common_time(Communicator, resolution=args.resolution, time_func=time_func)
  
    # Enabling Emon
    emon_target_output_path = os.path.join(target_dir, emon_output_filename)
    emon_pid = enable_emon(emon_cmd_params=emon_cmd_params, emon_target_output_path=emon_target_output_path)
 
    # Run Thermapy - launch PythonSV and then start collecting data
    thermapy_raw_data_path = os.path.join(host_dir, thermapy_output_filename)
    thermapy_process = enable_thermapy(ip, thermapy_raw_data_path, data_collection_duration, thermapy_launching_duration, thermapy_lab_path, args.resolution, thermapy_launch_path)
    # todo hack:  need to leave time for daq align at the end
    #daq_t1 = time.time()
    time.sleep(1)
    try:
        Communicator.ExecuteCommandOnTarget(command=align_cmd, sCommandCwd=align_dir)
    except:
        pass
    time.sleep(1)
    # Run WL
    # wl_cmd = f"timeout 1; timeout 1; {wl_cmd}"
    wl_pid = Communicator.ExecuteCommandOnTargetAsync(command=wl_cmd, bOrphan=False, sCommandCwd=wl_dir)
    wl_start_time = time.time()
    print(f"WL PID={wl_pid}")
    print(thermapy_process.is_alive())
    # Keep running thermapy as long as workload running
    # while thermapy_process.is_alive() and time.time() - daq_t1 <= 40:
        # try:
            # res = Communicator.ExecuteCommandOnTarget(f'tasklist | find "{wl_pid}"', logOutput=False) 
        # except:
            # break
            
    while True:
        try:
            res = Communicator.ExecuteCommandOnTarget(f'tasklist | find "{wl_pid}"', logOutput=False) 
        except:
            break
        if wl_duration is not None and time.time() - wl_start_time >= wl_duration:
            break
            
    Communicator.KillCommandOnTarget(pid=str(wl_pid))
    print('Terminating WL...')
    time.sleep(1)
    try:
        Communicator.ExecuteCommandOnTarget(command=align_cmd, sCommandCwd=align_dir)
    except:
        pass
    time.sleep(1)
    thermapy_process.kill()
    print('Terminating Thermapy...')
    time.sleep(1)
    thermapy_process.join()
    # os.remove(thermapy_launch_path)
    print(f'File was removed? {os.path.isfile(thermapy_launch_path)}')
    
    Communicator.KillCommandOnTarget(pid=str(emon_pid))
    print('Terminating Emon...')
    time.sleep(1)
    if Communicator.IsFile(path=emon_target_output_path):
        print(f"Emon created a trace file of size: {Communicator.GetFileSize(sFilePath=emon_target_output_path)}")
        
        # Sync files between host & target
        emon_host_output_path = os.path.join(host_dir, emon_output_filename)
        Communicator.GetFileFromTarget(sourceFileLocation=emon_target_output_path, whereToStore=emon_host_output_path)
        print(f"{emon_target_output_path} -> {emon_host_output_path}")
        
        # Remove output dir in target
        Communicator.ExecuteCommandOnTarget(command=f'rmdir /S /Q {target_dir}')  
        
    # Parsing Thermapy raw data
    thermapy_parsed_output_file = os.path.join(host_dir, f'parsed_{thermapy_output_filename}')
    thermapy_post_processing(thermapy_lab_path, thermapy_raw_data_path, thermapy_parsed_output_file)
    print("Finished.")
    