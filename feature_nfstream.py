# import multiprocessing
import time
import nfstream
print(nfstream.__version__)

import pandas as pd
import glob, os
import numpy as np
from nfstream import NFStreamer, NFPlugin
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 50)

def pcap_filepath(path,device_label):
    file_path=path+'/'+device_label+'/**/*.pcap'
    all_files = glob.glob(file_path,recursive = True)
    return all_files

def get_device_list(path):
    p = os.listdir(path)
    device_list=[]
    for i in p:
        x = path+'/'+i
        if os.path.isdir(x):
            device_list.append(i)
            #print(x)
    return device_list

def add_label(df,device_name, label):
    df1= df.copy()
    df1['device'] = device_name
    df1['label'] = label
    
    df1.reset_index(drop = True, inplace = True)
    columns_name = df1.columns
    dflist = df1.values.tolist() #convert df to list
    return dflist, columns_name

#feature extract, label and combine 
def data_preparation(inpath,outpath,label):
    #label =1
    am=1

    device_list = get_device_list(inpath)
    i=0
    
    while i < len(device_list):
        mylist = []
        j=0
        filepath = pcap_filepath(inpath,device_list[i])
        for filename in filepath:
            print(filename)

            #df = NFStreamer(filename,statistical_analysis=True,accounting_mode=am,idle_timeout=120,active_timeout=1800,).to_pandas() #s1
            df = NFStreamer(filename,statistical_analysis=True,accounting_mode=am,idle_timeout=10,active_timeout=30,).to_pandas() #s3
            #df = NFStreamer(filename,statistical_analysis=True,accounting_mode=am,idle_timeout=60,active_timeout=60,).to_pandas()   #s2_1
            #df = NFStreamer(filename,statistical_analysis=True,accounting_mode=am,idle_timeout=120,active_timeout=120,).to_pandas()   #s2_2
            #df = NFStreamer(filename,statistical_analysis=True,accounting_mode=am,idle_timeout=240,active_timeout=240,).to_pandas()   #s2_4
            #df = NFStreamer(filename,statistical_analysis=True,accounting_mode=am,idle_timeout=120,active_timeout=480,).to_pandas()   #s2_8
            if(df is None):
                continue
            
            # add label(0,1:non,iot) and device name
            dl, columns_name = add_label(df,device_list[i],label)
            

            # combine
            mylist.extend(dl)
            j+=1
        
        print('combined ',j,' ', device_list[i],' pcap files') # done with one device 
        
        
        mydf = pd.DataFrame(mylist, columns=columns_name) #change list to dataframe
        mydf.to_csv(outpath+'/'+device_list[i]+'.csv')  # save to csv
        
        # mydf.to_csv('/mnt/c/Users/onewa/OneDrive - Universiti Malaya/dataset/iotfsktm/Combined CSV/nfstream/iot_dataset_s3.csv', index=False)
        i+=1
    print('all ',i,' devices combined')
    
    #return mydf


 

if __name__ == '__main__':
    tic = time.time()

    inpath = "/mnt/c/Users/onewa/OneDrive - Universiti Malaya/dataset/iotfsktm/By Devices"
    outpath= "/mnt/c/Users/onewa/OneDrive - Universiti Malaya/dataset/iotfsktm/Combined CSV/nfstream"

    dlist = get_device_list(inpath) #list all device
    print(dlist)
    
    df_iot = data_preparation(inpath,outpath,1) #extract feature
    #save df to csv
    #p = Process(target=data_preparation, args=('bob',))
    #p.start()
    #p.join()


    # processes = []
    # for idx, trace in enumerate(dlist):
    #     pool = multiprocessing.Pool(5)
    #     pool.map_async(data_preparation, (idx,), dict(device_list=[trace]))
    #     pool.close()


        #print(trace)
        # p = multiprocessing.Process(target=data_preparation,args=(idx,[trace],))
        # processes.append(p)
        # p.start()

    # for process in processes:
    #     process.join()


    toc = time.time()
    #df_iot.to_csv('/mnt/e/OneDrive/1-Documents/1-PhD/2-Experiment/data/sentinel/iot_dataset_s3.csv', index=False)
    print('Done in {:.4f} seconds'.format(toc-tic))