from datetime import datetime, timedelta 
from enum import Enum
import re, sys, os, subprocess, json
from types import ModuleType
from typing import Dict, List, Any
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from croniter import croniter 
 

class Framework(ModuleType):
    class Controller:
        args: Dict[str, Any]
        strem_nm: str
        prcs_grp: list
        prcs_grp_prir: str
        prcs_grp_act_f: list
        prcs_nm: list
        prcs_prir: list
        src_schm_nm: str
        src_tbl: str
        tgt_schm_nm: str
        tgt_tbl: str
        prcs_typ: int
        af_path_nm: str
        af_parm: str
        prcs_act_f: list
        dpnd_prcs_nm: list
        depn_chk: int
        depn_act_f: list
        calc_dt : datetime
        cron_express : str
        owner : str

        def __init__(self, args: dict, flag_type):
            if flag_type == 'strem_nm':
                self.strem_nm = args['strem_nm']
                self.prcs_grp = args['prcs_grp']
                self.prcs_grp_prir = args['prcs_grp_prir']
                self.prcs_grp_act_f = args['prcs_grp_act_f']
                self.cron_express = args['cron_express']
                self.owner = args['owner']
                self.calc_dt =  croniter(args['cron_express'], datetime.now()).get_prev(datetime)
            elif flag_type == 'prcs_grp':
                self.prcs_grp = args['prcs_grp']
                self.prcs_nm = args['prcs_nm']
                self.prcs_prir = args['prcs_prir']
                self.prcs_act_f = args['prcs_act_f']
            elif flag_type == 'prcs_nm':
                self.prcs_nm = args['prcs_nm'][0]
                self.prcs_grp = args['prcs_grp']
                self.prcs_prir = args['prcs_prir']
                self.src_schm_nm = args['src_schm_nm']
                self.src_tbl = args['src_tbl']
                self.tgt_schm_nm = args['tgt_schm_nm']
                self.tgt_tbl = args['tgt_tbl']
                self.prcs_typ = args['prcs_typ']
                self.af_path_nm = args['af_path_nm']
                self.af_parm = args['af_parm']
                self.prcs_act_f = args['prcs_act_f']
                self.dpnd_prcs_nm = args['dpnd_prcs_nm']
                self.depn_act_f = args['depn_act_f']
                self.owner = args['owner']
                self.calc_dt = croniter(args['cron_express'], datetime.now()).get_prev(datetime)
            
    class InitController(Controller):
        def __init__(self, args: dict, flag_type):
            if len(args) != 0:
                super().__init__(args, flag_type)

    @classmethod
    def get_controller(self, flag_value: str = None, flag_type: str = None) -> Controller:
        # purpose is to call init of controller
        #1.retrieve config from postgres
            #fetchone(): Fetches a single row from the result set.
            #fetchall(): Fetches all rows from the result set.
            #fetchmany(size): Fetches a specified number of rows from the result set.
        conn = Framework.Utility.GetConnection()
        cusor = conn.cursor()
        if flag_type == 'strem_nm':
            cusor.execute(f"""select 
                            strem.strem_nm
                            , prcs_grp.prcs_grp
                            , prcs_grp.prir as prcs_grp_prir
                            , prcs_grp.act_f as prcs_grp_act_f
                            , COALESCE(strem.owner, 'test-framework') as owner
                            , concat(schedule.minutes, ' ', schedule.hours, ' ', schedule.day_month, ' ', schedule.months, ' ', schedule.day_week) as cron_express
                            from cntl_af.cntl_cfg_strem strem
                            left join cntl_af.cntl_cfg_prcs_grp prcs_grp  
                                on strem.strem_nm = prcs_grp.strem_nm 
                            left join cntl_af.cntl_cfg_schedule schedule
                                on strem.strem_nm = schedule.prcs_nm 
                            where strem.strem_nm = '{flag_value}'
                            order by prcs_grp_prir                 
                            """)
        elif flag_type == 'prcs_grp':
            cusor.execute(f"""select 
                            prcs_grp.prcs_grp 
                            , prcs.prcs_nm 
                            , prcs.prir as prcs_prir
                            , prcs.act_f as prcs_act_f
                            from cntl_af.cntl_cfg_prcs_grp prcs_grp 
                            left join  cntl_af.cntl_cfg_prcs prcs 
                                on prcs_grp.prcs_grp = prcs.prcs_grp
                            where prcs.prcs_grp = '{flag_value}'
                            order by prcs_prir
                            """)
        elif flag_type == 'prcs_nm':
                       cusor.execute(f"""select 
                            prcs.prcs_nm
                            , prcs.prcs_grp
                            , prcs.prir as prcs_prir
                            , prcs.src_schm_nm
                            , prcs.src_tbl
                            , prcs.tgt_schm_nm
                            , prcs.tgt_tbl
                            , prcs.prcs_typ
                            , prcs.af_path_nm
                            , prcs.af_parm
                            , prcs.act_f as prcs_act_f
                            , depn.dpnd_prcs_nm
                            , depn.act_f as depn_act_f
                            , COALESCE(strem.owner, 'test-framework') as owner
                            , concat(schedule.minutes, ' ', schedule.hours, ' ', schedule.day_month, ' ', schedule.months, ' ', schedule.day_week) as cron_express
                            from cntl_af.cntl_cfg_prcs prcs   
                            left join cntl_af.cntl_cfg_prcs_depn depn
                                on prcs.prcs_nm = depn.prcs_nm 
                            left join cntl_af.cntl_cfg_prcs_grp prcs_grp 
                                on prcs.prcs_grp = prcs_grp.prcs_grp 
                            left join cntl_af.cntl_cfg_strem strem
                                on prcs_grp.strem_nm = strem.strem_nm
                            left join cntl_af.cntl_cfg_schedule schedule
                                on strem.strem_nm = schedule.prcs_nm
                            where prcs.prcs_nm = '{flag_value}'
                            """)
                       
        header_row = [i[0] for i in cusor.description]
        data_row = cusor.fetchall() # [(),(), ...]

        #2.connect arttribute by joining and conbine to json or dict format configuration pass
        config_data = dict()
        for hr_ind in range(len(header_row)):
            for dr_ind in range(len(data_row)): 
                if header_row[hr_ind] in ('prcs_grp','prcs_nm','dpnd_prcs_nm','prcs_grp_act_f', 'prcs_act_f', 'depn_act_f','prcs_prir','prcs_grp_prir') and  header_row[hr_ind] not in config_data:
                    config_data[header_row[hr_ind]] = [data_row[dr_ind][hr_ind]]
                elif header_row[hr_ind] in ('prcs_grp','prcs_nm','dpnd_prcs_nm','prcs_grp_act_f', 'prcs_act_f', 'depn_act_f','prcs_prir','prcs_grp_prir') and  header_row[hr_ind] in config_data:
                    config_data[header_row[hr_ind]].append(data_row[dr_ind][hr_ind])
                elif header_row[hr_ind] not in config_data:
                    config_data[header_row[hr_ind]] = data_row[dr_ind][hr_ind]

        #3.initial config of airflow dag
        # return config_data
        return Framework.InitController(config_data, flag_type)


    class Utility(ModuleType):
        @classmethod
        def GetConnection(self):
            hook = PostgresHook(postgres_conn_id='postgres_localhost')
            conn = hook.get_conn()
            return conn

        @classmethod
        def CheckRunProcess(self, process_name, calculate_date):
            conn = Framework.Utility.GetConnection()
            cusor = conn.cursor()            
            cusor.execute(f"""select * 
                          from cntl_af.cntl_cfg_log
                          where start_dt = date('{calculate_date}') and prcs_nm = '{process_name}'
                        """)
            if len(cusor.fetchall()) == 0:
                 return False
            
            return True
        
        @classmethod
        def InsertLogProcess(self, process_name, start_dt, end_dt, status, message='', source_row=0, target_row=0):
            conn = Framework.Utility.GetConnection()
            cusor = conn.cursor() 
            query = """
                    INSERT INTO cntl_af.cntl_cfg_log
                    (prcs_nm, start_dt, end_dt, status, message, source_row, target_row, upt_dt)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                    """
            values = (process_name, start_dt, end_dt, status, message, source_row, target_row)
            cusor.execute(query, values)
            conn.commit() 


        def UpdateLogProcess(process_name, start_dt, end_dt, status, message='', source_row=0, target_row=0):
            conn = Framework.Utility.GetConnection()
            cusor = conn.cursor() 
            query = """
                    UPDATE cntl_af.cntl_cfg_log
                    SET end_dt = %s, status = %s, message = %s
                    where prcs_nm = %s and start_dt = %s
                    """
            values = (end_dt, status, message, process_name, start_dt)
            cusor.execute(query, values)
            conn.commit() 


        @classmethod
        def GetContainerId(self):
            try:
                container_id = subprocess.check_output(
                    "docker ps -q --filter 'name=spark-iceberg'", 
                    shell=True
                ).decode('utf-8').strip()  
                return container_id
            except subprocess.CalledProcessError as e:
                raise(f"Error getting container ID: {e}")
            

        
             
 