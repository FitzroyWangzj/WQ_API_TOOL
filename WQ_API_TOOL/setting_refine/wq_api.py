import os
import shutil
import pandas as pd
import numpy as np
import requests
from concurrent.futures import ThreadPoolExecutor
import ast
import csv
import json
from time import sleep
import time

username = ""
password = ""


def sign_in(username,password):
    import pandas as pd
    import requests
    from os import environ

    # Create a session to persistently store the headers
    s = requests.Session()

    # Save credentials into session
    s.auth = (username, password)

    while True:
        try:
            # Send a POST request to the /authentication API
            response = s.post('https://api.worldquantbrain.com/authentication')
            response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
            break

        except:
            print("connection down, trying to login again")
            sleep(10)

    print(response)
    return s

def get_ls_count(session,alpha_id):
    while True:
        try:
            recordsets = session.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/recordsets/yearly-stats")

            data_dict = recordsets.json()

                    # Get the index of 'longCount' and 'shortCount' in the properties list

            long_count_index = next(index for (index, d) in enumerate(data_dict['schema']['properties']) if d["name"] == "longCount")

            short_count_index = next(index for (index, d) in enumerate(data_dict['schema']['properties']) if d["name"] == "shortCount")

            # Extract 'longCount' and 'shortCount' data from records

            long_count = [record[long_count_index] for record in data_dict['records']]

            short_count = [record[short_count_index] for record in data_dict['records']]

            return long_count,short_count

        except:

            print(f"error occured when get the {alpha_id}'s record sets, sleep 5 second")

            sleep(5)

def get_universe_coverage(session,settings):
    simulation_data = {
        'type': 'REGULAR',
        'settings': {
            'instrumentType': 'EQUITY',
            'region': settings['region'],
            'universe': settings['universe'],
            'delay': settings['delay'],
            'decay': 0,
            'neutralization': 'NONE',
            'truncation': 0.01,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'OFF',
            'language': 'FASTEXPR',
            'visualization': False,
        },
        'regular': "close" #the close whoud have the highest coverage in the chosen universe
    }
    while True:
        try:
            response = session.post('https://api.worldquantbrain.com/simulations', json=simulation_data)
            break
        except:
            session = sign_in(username, password)
            print("Error in sending simulation request. And retry after 5s")
            sleep(5)
    while True:
        try:
            alpha_id = session.get(response.headers["Location"]).json()['alpha']
            long_count,short_count = get_ls_count(session=session, alpha_id=alpha_id)
            universe_coverage = long_count[-1] + short_count[-1]
            break
        except:
            print("Error in getting the universe coverage. And retry after 5s")
            sleep(5)
    return universe_coverage

def get_datafields(dataset_id_ls):
    datafields_df_ls =[]
    for i in dataset_id_ls:
        datafields_df = get_datafields(session,dataset_id=i,region=setting['region'],universe=setting['universe'],delay=setting['delay'])
        datafields_df = datafields_df[datafields_df['type']=="MATRIX"]['id']
        datafields_df_ls.append(datafields_df)

    # 使用 concat 将所有数据框合并为一个
    total_datafields_df = pd.concat(datafields_df_ls, ignore_index=True)
    datafields = total_datafields_df.values.tolist()
    return datafields

def get_datafields(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000',
    dataset_id: str = '',
    search: str = ''
):
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" +\
            "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count'] 
    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" +\
            f"&search={search}" +\
            "&offset={x}"
        count = 100
    
    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df

# Function to fetch a specific number of IS alphas (`total_alphas`) using pagination.
def get_n_is_alphas(session, total_alphas, limit=100):
    fetched_alphas = []
    offset = 0

    # Keep fetching alphas until the required number of alphas is reached or no more alphas are available.
    while len(fetched_alphas) < total_alphas:
        response = session.get(
            f"https://api.worldquantbrain.com/users/self/alphas?stage=IS&limit={limit}&offset={offset}"
        )
        alphas = response.json()["results"]
        fetched_alphas.extend(alphas)
        if len(alphas) < limit:
            break
        offset += limit
    return fetched_alphas[:total_alphas]

# 创建文件夹
def build_folder(dataset_id, settings, override_mode=False):
    # 创建文件夹名
    folder_name = f"{dataset_id}_{settings['region']}_{settings['universe']}_Delay{settings['delay']}"

    # 检查文件夹是否已经存在
    if os.path.exists(folder_name):

        # 如果override_mode为True，删除文件夹内所有内容，即创建空文件夹
        if override_mode:
            shutil.rmtree(folder_name)
            os.makedirs(folder_name)

        else:
            print(f"Folder '{folder_name}' already exists. No action taken.")

    else:
        # 如果文件夹不存在，创建它
        os.makedirs(folder_name)

    #return the folder path
    return folder_name

# 在文件夹中为每个数据字段创建一个csv
def setup_datafield_csv(datafields_df, folderpath):
    for index, row in datafields_df.iterrows():
        # Create filename using 'id' column values
        filename = f"{row['id']}_{(row['type']).lower()}.csv"
        filepath = os.path.join(folderpath, filename)

       

        # Check if the file already exists

        if os.path.isfile(filepath):

            print(f"File '{filename}' already exists. Skipping...")

            continue

       

        # Create a new DataFrame to store this row's information

        df = pd.DataFrame([row])

       

        # Modify the 'id' value in the DataFrame based on the 'type' value

        if row['type'] == 'VECTOR':

            df.loc[index, 'id'] = f"vec_choose({row['id']}, nth=-1)"

        elif row['type'] != 'MATRIX':

            print(f"ID: {row['id']} is not VECTOR or MATRIX type. Skipping...")

            continue

       

        # Write the DataFrame to a CSV file

        df.to_csv(filepath, index=False)

    print("Setup complete.")

# 打开每个csv并附上表达式 go to every csv file in the folderpath
def generate_alpha_in_csv(folderpath):
  # Iterate over each file in the folder
  for filename in os.listdir(folderpath):

      # Check if the file is a CSV file
      if filename.endswith('.csv'):
          filepath = os.path.join(folderpath, filename)

          # Read the first row of the CSV file
          df = pd.read_csv(filepath, nrows=1)

          if "alpha_location" in df.columns:
              print(f"Alpha expressions have been created in this {filename}, skip")
              continue

          datafield = f"last_diff_value({df.iloc[0]['id']},150)"

          # 生成alpha1:是否有整数？是否全是整数？
          df = df._append({'id':f"if_else(({datafield} - floor({datafield}))==0,1,0)"}, ignore_index=True)  

          # 生成alpha2:绝对值是否在0-1区间
          df = df._append({'id':f"if_else(((abs({datafield})<=1) && (abs({datafield})>=0)),1,0)"}, ignore_index=True)  

          # 生成alpha3:测试偏度是否正常 -3《=值《=3
          df = df._append({'id':f"skewness =ts_mean((group_sum(power(({datafield}-group_mean({datafield},1,country))/group_std_dev({datafield},country),3),country)*(group_count({datafield},country)/(group_count({datafield},country)-1)/(group_count({datafield},country)-2))),200);if_else(skewness<=3 && skewness>= -3,1,0)"}, ignore_index=True)  

          # 生成alpha4:测试偏度是否右偏 值》3，自然地，如果不是右偏，就是左偏了
          df = df._append({'id':f"skewness =ts_mean((group_sum(power(({datafield}-group_mean({datafield},1,country))/group_std_dev({datafield},country),3),country)*(group_count({datafield},country)/(group_count({datafield},country)-1)/(group_count({datafield},country)-2))),200);if_else(skewness> 3,1,0)"}, ignore_index=True)  

          # 生成alpha5:测试峰度是否正常 2<=值<=5
          df = df._append({'id':f"kurtosis = ts_mean(((group_sum(power(({datafield} - group_mean({datafield},1,country))/group_std_dev({datafield},country),4),country)* (group_count({datafield},country)*(group_count({datafield},country)+1)/(group_count({datafield},country)-1)/(group_count({datafield},country)-2)/(group_count({datafield},country)-3))) - (3* power(group_count({datafield},country)-1,2)/(group_count({datafield},country)-2)/(group_count({datafield},country)-3))),200);if_else(kurtosis<=5 && kurtosis>=2,1,0)"}, ignore_index=True)  

          # 生成的alpha6: 测试峰度是否高峰 值》5，自然地，如果不是高峰，就是扁平了
          df = df._append({'id':f"kurtosis = ts_mean(((group_sum(power(({datafield} - group_mean({datafield},1,country))/group_std_dev({datafield},country),4),country)* (group_count({datafield},country)*(group_count({datafield},country)+1)/(group_count({datafield},country)-1)/(group_count({datafield},country)-2)/(group_count({datafield},country)-3))) - (3* power(group_count({datafield},country)-1,2)/(group_count({datafield},country)-2)/(group_count({datafield},country)-3))),200);if_else(kurtosis>5,1,0)"}, ignore_index=True)  

          # 生成的alpha7: 测试是否日度更新
          df = df._append({'id':f"ts_std_dev({datafield},2) != 0 ? 1 : 0"}, ignore_index=True)  

          # 生成的alpha8: 测试是否周度更新
          df = df._append({'id':f"ts_std_dev({datafield},5) != 0 ? 1 : 0"}, ignore_index=True)  

          # 生成的alpha9: 测试是否月度更新
          df = df._append({'id':f"ts_std_dev({datafield},22) != 0 ? 1 : 0"}, ignore_index=True)  

          # 生成的alpha10: 测试是否季度更新
          df = df._append({'id':f"ts_std_dev({datafield},66) != 0 ? 1 : 0"}, ignore_index=True)  

          # 生成的alpha11: 测试是否半年度更新，如果都不是，就是年度更新了
          df = df._append({'id':f"ts_std_dev({datafield},122) != 0 ? 1 : 0"}, ignore_index=True)  

          # 生成后续的Alpha以测试数值的分布。
          ranges = [(0, 0.05), (0.05, 0.1), (0.1, 0.15), (0.15, 0.2),  # Sub-ranges for 0 to 0.2
            (0.2, 0.25), (0.25, 0.3), (0.3, 0.35), (0.35, 0.4),  # Sub-ranges for 0.2 to 0.4
            (0.4, 0.45), (0.45, 0.5), (0.5, 0.55), (0.55, 0.6),  # Sub-ranges for 0.4 to 0.6
            (0.6, 0.65), (0.65, 0.7), (0.7, 0.75), (0.75, 0.8),  # Sub-ranges for 0.6 to 0.8
            (0.8, 0.85), (0.85, 0.9), (0.9, 0.95), (0.95, 1)]  # Sub-ranges for 0.8 to 1

          start_index = 12  # Assuming you want to start from the 13th row (0-based index)

          for i, (lower, upper) in enumerate(ranges, start=start_index):
            # for the last range, we need to add a condition to include the upper bound
            if i == len(ranges) + start_index - 1:
                df = df._append({'id':f"{lower} <= scale_down({datafield}) && scale_down({datafield}) <= {upper} ? 1 : 0"}, ignore_index=True)
            else:
                df = df._append({'id':f"{lower} <= scale_down({datafield}) && scale_down({datafield}) < {upper} ? 1 : 0"}, ignore_index=True)
          # add mone more column into the df

          df['alpha_location'] = ""
          df['alpha_id'] = ""
          df['long_count'] = ""
          df['short_count'] = ""

          # # Write the DataFrame back to the CSV file
          df['id'][0] = datafield
          df.to_csv(filepath, index=False)

          print(f"{datafield} done")

# 使用多线程，将alpha 的 id和ls count得到
def update_df(session, alpha_location, df, filepath):

    
    alpha_status = session.get(alpha_location).json()['status']
    if alpha_status == 'ERROR':
        long_count = [np.nan,np.nan, np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]
        short_count = [np.nan,np.nan, np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan]
        alpha_id = session.get(alpha_location).json()['id']
    else:
        alpha_id = session.get(alpha_location).json()['alpha']
        long_count,short_count = get_ls_count(session=session, alpha_id=alpha_id)
    # update id to the row
    df.loc[df['alpha_location'] == alpha_location, 'alpha_id'] = alpha_id

    df.loc[df['alpha_location'] == alpha_location, 'long_count'] = str(long_count)

    df.loc[df['alpha_location'] == alpha_location,'short_count'] = str(short_count)

    df.to_csv(filepath, index=False)

def send_alpha_to_sim(session,folderpath):

    for filename in os.listdir(folderpath):

        # Check if the file is a CSV file

        if filename.endswith('.csv'):

            alpha_list_with_settings = []

            filepath = os.path.join(folderpath, filename)

            # Read the first row of the CSV file

            df = pd.read_csv(filepath)

            expressions = df['id']

            if sum(df['alpha_location'].isna())==0 and (sum(df['alpha_location'].isna())==sum(df['long_count'].isna())):

                print(f"this {filename} has been tested, skip and go to next file")

                continue
            if (sum(df['long_count'].isna())==0):

                print(f"count result of {filename} has been tested, skip and go to next file")

                continue
            # create settings

            for alpha_expression in expressions:
                simulation_data = {

                    'type': 'REGULAR',

                    'settings': {

                        'instrumentType': 'EQUITY',

                        'region': df.iloc[0]['region'],

                        'universe': df.iloc[0]['universe'],

                        'delay': int(df.iloc[0]['delay']),

                        'decay': 0,

                        'neutralization': 'NONE',

                        'truncation': 0.01,

                        'pasteurization': 'ON',

                        'unitHandling': 'VERIFY',

                        'nanHandling': 'OFF',

                        'language': 'FASTEXPR',

                        'visualization': False,

                    },

                    'regular': alpha_expression #Quarterly fundamental item : Retained Earnings，

                }

                alpha_list_with_settings.append(simulation_data)

            for alpha_data in alpha_list_with_settings:

                while True:

                    try:

                        # Send multisimulation request

                        response = session.post('https://api.worldquantbrain.com/simulations', json=alpha_data)

                        # Check response

                        if len(response.headers["Location"]) >0:

                            print("Alpha location get successfully.")

                            df.loc[df['id'] == alpha_data['regular'], 'alpha_location'] = response.headers["Location"]

                            df.to_csv(filepath, index=False)

                            print("Alpha location added to the CSV file.")

                            break

                    except:

                        session = sign_in(username, password)

                        print("Error in sending simulation request. And retry after 5s")

                        sleep(5)

            df = pd.read_csv(filepath)

            while True:

                if (sum(df['long_count'].isna())==0):

                    break

                else:

                    with ThreadPoolExecutor() as executor:

                        alpha_location_list = list(df['alpha_location'])    

                        if (sum(df['long_count'].isna())==0):

                            print(f"count result of {filename} has been tested, skip and go to next file")

                            continue

                        for alpha_location in alpha_location_list:

                            executor.submit(update_df, session, alpha_location, df, filepath)

                    sleep(5)

    print("all alphas has been sent to test")

def simulate(session,alpha_list_with_settings,sublist_size,t_max,sleep_time):
    # 记录开始时间
    start_time = time.time()

    t = 0
    total_length = len(alpha_list_with_settings)
    for i in range(0, total_length, sublist_size):
        sublist = alpha_list_with_settings[i:i + sublist_size]
        while True:
            try:
                if t >= t_max:
                    print(f"wait for {sleep_time}s")
                    sleep(sleep_time)
                    t = 0
                
                # 计算运行时间
                elapsed_time = time.time() - start_time

                # 如果运行时间超过4小时，重新登录
                if (elapsed_time > 4 * 3600):
                    session = sign_in(username, password)
                    start_time = time.time()  # 重置开始时间
                    print("Session renewed successfully.")

                # 发送多重模拟请求
                response = session.post('https://api.worldquantbrain.com/simulations', json=sublist)

                # 检查响应
                if len(response.headers["Location"]) > 0:
                    print("Alpha location get successfully.")
                    print(response.headers["Location"])
                    t = t + 1
                    break

            except Exception as e:
                # 打印错误信息
                print(f"Error in sending simulation request. Retrying after 5s. Error: {e}")
                sleep(5)

    # 处理剩余的元素（如果有）
    if total_length % sublist_size != 0:
        remaining_sublist = alpha_list_with_settings[-(total_length % sublist_size):]
        t = t + 1
        while True:
            try:
                if t >= t_max:
                    print(f"wait for {sleep_time} s")
                    sleep(sleep_time)
                    t = 0

                # 计算运行时间
                elapsed_time = time.time() - start_time

                # 如果运行时间超过4小时，重新登录
                if (elapsed_time > 4 * 3600):
                    session = sign_in(username, password)
                    start_time = time.time()  # 重置开始时间
                    print("Session renewed successfully.")

                # 发送多重模拟请求
                response = session.post('https://api.worldquantbrain.com/simulations', json=remaining_sublist)
                # 检查响应
                if len(response.headers["Location"]) > 0:
                    print("Alpha location get successfully.")
                    print(response.headers["Location"])
                    break

            except Exception as e:
                # 打印错误信息
                print(f"Error in sending simulation request. Retrying after 5s. Error: {e}")
                sleep(5)

def get_simulate_result(session,n_alpha,dir_path,from_path,remain_path):
    1

def submittable(alpha):
    # Check if 'is' and 'checks' exist in the alpha
    if 'is' in alpha and 'checks' in alpha['is']:
        for check in alpha['is']['checks']:
            if check['result'] == 'FAIL':
                return False
        return True
    else:
        # If 'is' or 'checks' is not present, we cannot determine the submittable status
        return False

def get_id_with_result(session,folderpath):
    with ThreadPoolExecutor() as executor:
        for filename in os.listdir(folderpath):
            if filename.endswith('.csv'):
                alpha_location_list = []
                filepath = os.path.join(folderpath, filename)
                df = pd.read_csv(filepath)
                alpha_location_list = list(df['alpha_location'])    
                if (sum(df['long_count'].isna())==0):
                    print(f"count result of {filename} has been tested, skip and go to next file")
                    continue
                for alpha_location in alpha_location_list:
                    executor.submit(update_df, session, alpha_location, df, filepath)
            print(f"finished retriving result for {filename}")
      
def get_prod_corr(session, alpha_id):
    try:
        response = session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}/correlations/prod")
        response.raise_for_status()
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            print(f"Error while decoding prod_corr JSON for alpha {alpha_id}: Invalid or empty JSON")
            return pd.DataFrame(columns=["alphas", "max"])

        if "records" in response_data:
            columns = [dct["name"] for dct in response_data["schema"]["properties"]]
            prod_corr_df = pd.DataFrame(response_data["records"], columns=columns)
            return prod_corr_df
    except requests.HTTPError as e:
        print(f"Error while fetching prod_corr for alpha {alpha_id}: {e}")
    return pd.DataFrame(columns=["alphas", "max"])  

def get_prod_corr_with_retries(session, alpha_id, max_retries=3):
    retry_count = 0
    while retry_count < max_retries:
        try:
            prod_corr_df = get_prod_corr(session, alpha_id)
            if not prod_corr_df.empty:
                return prod_corr_df
        except json.JSONDecodeError:
            pass
        retry_count += 1
        time.sleep(1)  # Wait for 1 second before the next retry
    return pd.DataFrame(columns=["alphas", "max"])
    
def get_pnl(session,alpha_id):
    while True:
        pnl = session.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/recordsets/pnl")
        if pnl.headers.get("Retry-After", 0) == 0:
            break
        print("Sleeping for " + pnl.headers["Retry-After"] + " seconds")
        sleep(float(pnl.headers["Retry-After"]))
    print("PnL retrieved")
    return pnl

def rolling_sharpe(returns,window=252):
    mean_returns = returns.rolling(window).mean()
    std_returns = returns.rolling(window).std()
    sharpe_ratio = mean_returns / std_returns
    return sharpe_ratio










