import csv
from databaseClass import SQLiteDatabase as db
import subprocess
import os
import networkx as nx
from Class_Task_Mappings import Task_Mappings as tm

DATABASE_NAME = 'eeg_functions.sqlite'
# TODO
# 1. Put Numbers on the folder Names
# 2. Fgure out how to do naming conventions for branching 

class SnakeControl:
    def __init__(self,user_recording_files,user_dag_dict):
        super().__init__()
        
        # user variables
        self.recording_files = user_recording_files
        self.dag_dict = user_dag_dict
        self.dag = self.dag_create_dag_from_dict()
        self.output_steps = []
        
        # Snakemake variables
        self.num_cores = "3"
        self.latency_wait = "60"
        self.SnakeMake_FileName = "Snakefile"
        
        self.snakefile_content = ""
    
    # python -c "from Test_Matlab_engine import Test_class; result = Test_class.HighpassFilterTest(); print(result);"
    
    def run(self):
        self.create_database_snakeMake()
        db.execute_query(DATABASE_NAME, 'DROP TABLE IF EXISTS snakemake_rules')
        db.execute_query(DATABASE_NAME, '''
        CREATE TABLE IF NOT EXISTS snakemake_rules
        (id INTEGER PRIMARY KEY, slot_id INTEGER, snakemake_rule TEXT)
        ''')
        top_order = self.dag_topological_order()
        
        for current_row, func in enumerate(top_order):
            sql_query = "SELECT * FROM eeg_functions WHERE function_display_name = ?"
            func_details = db.execute_query(DATABASE_NAME, sql_query, (func,))
            func_mode = func_details[0][5]
            output_folder = func.replace(" ", "_")
            
            prevFunc = self.dag_find_parent(func)
            
            if prevFunc == None:
                print("No parent")
                input_folder = "raw"
            else:                 
                input_folder = prevFunc.replace(" ", "_")
                
            if func_mode == 'auto':
                self.dag_generate_snakemake_rules(current_row, input_folder, output_folder,func_details)
            elif func_mode == 'manual':
                print('manual function detected')
                print('executing till: ', func)
                
                sinks = [prevFunc]
                
                self.dag_generate_output_rule(sinks, prevFunc)
                prevFuncStepName = prevFunc.replace(" ", "_")
                self.output_steps.append(prevFuncStepName)
                
            else:
                print("Error")
                exit()
        sinks = self.dag_find_sinks()
        self.output_steps.append('Final_Step')
        self.dag_generate_snakemake_file(sinks)
        self.run_SnakeMake()
            
    def run_SnakeMake(self):
        for step in self.output_steps:
            print("Running step:", step)
            # Define the Snakemake command
            command = "snakemake --cores "+ self.num_cores + " --latency-wait " + self.latency_wait + " --keep-going  --ignore-incomplete --prioritize " + step + " -s " + self.SnakeMake_FileName 
            # Run the command
            subprocess.run(command, shell=True)
        
    
    def dag_create_dag_from_dict(self):
        dag = nx.DiGraph()
        for parent, children in self.dag_dict.items():
            for child in children:
                dag.add_edge(parent, child)
        return dag

    def dag_topological_order(self):
        try:
            return list(nx.topological_sort(self.dag))
        except nx.NetworkXUnfeasible:
            return None  # The graph contains a cycle

    def dag_find_parent(self, node):
        predecessors = list(self.dag.predecessors(node))
        if predecessors:
            return predecessors[0]
        return None

    def dag_find_children(self, node):
        return list(self.dag.successors(node))
    
    def dag_find_sinks(self):
        sinks = [node for node in self.dag.nodes() if not any(self.dag.successors(node))]
        return sinks
        
    def create_database_snakeMake(self):
        
        tm.input_dict_into_sql()
        # Drop the table if it already exists
        db.execute_query(DATABASE_NAME, 'DROP TABLE IF EXISTS snakemake_rules')

        # create snakemake rule table
        db.execute_query(DATABASE_NAME, '''
        CREATE TABLE IF NOT EXISTS snakemake_rules
        (id INTEGER PRIMARY KEY, slot_id INTEGER, snakemake_rule TEXT)
        ''')

        
    def fetch_snakemake_rules():
        result = db.execute_query(DATABASE_NAME, "SELECT snakemake_rule FROM snakemake_rules")
        rules = [row[0] for row in result]
        return rules
    
    def dag_generate_output_rule(self, sinks, rule_name):
        rule_name = rule_name.replace(" ", "_")
        output_file_list = []
        for sink in sinks:
            sink_folderName = sink.replace(" ", "_")
        
            sql_query = "SELECT * FROM eeg_functions WHERE function_display_name = ?"
            
            sink_details = db.execute_query(DATABASE_NAME, sql_query, (sink,))
            
            for file in self.recording_files:
                file_name_without_extension = os.path.splitext(os.path.basename(file))[0]
                output_file_list.append('data/' + sink_folderName + '/' + file_name_without_extension + sink_details[0][7])
        
        self.snakefile_content += f"""rule {rule_name}:
    input: {output_file_list}
    
"""
    
    def dag_generate_snakemake_rules(self,current_row, input_folder,output_folder, func_details):
        for file in self.recording_files:
            file_name_without_extension = os.path.splitext(os.path.basename(file))[0]
            input_file = '"' + 'data/' + input_folder + '/' + file_name_without_extension + func_details[0][6] + '"'
            output_file = '"' +'data/' + output_folder + '/' + file_name_without_extension + func_details[0][7] + '"'
            shell_command = '"""' + '\n' + '\t\t' + func_details[0][4] + '\n' + '\t\t' + '"""'
            rule_name = output_folder + '_' + file_name_without_extension
            snakemake_rule = f"""rule {rule_name}:
    input:
        {input_file}
    output:
        {output_file}
    shell:
        {shell_command}
    """
            # SQL query and data
            sql_query = "INSERT INTO snakemake_rules (slot_id, snakemake_rule) VALUES (?, ?)"
            data_to_insert = (current_row, snakemake_rule)

            # Store in the database
            db.execute_query(DATABASE_NAME, sql_query, data_to_insert)  # Execute query with data
            

    def dag_generate_snakemake_file(self,sinks):
        output_file_list = []
        for sink in sinks:
            sink_folderName = sink.replace(" ", "_")
        
            sql_query = "SELECT * FROM eeg_functions WHERE function_display_name = ?"
            
            sink_details = db.execute_query(DATABASE_NAME, sql_query, (sink,))
            
            for file in self.recording_files:
                file_name_without_extension = os.path.splitext(os.path.basename(file))[0]
                output_file_list.append('data/' + sink_folderName + '/' + file_name_without_extension + sink_details[0][7])

            self.snakefile_content += f"""rule {'Final_Step'}:
        input: {output_file_list}
        
"""
        
        sql_query = "SELECT snakemake_rule FROM snakemake_rules"
        snakemake_rules_rows =  db.execute_query(DATABASE_NAME, sql_query)
        for row in snakemake_rules_rows:
            self.snakefile_content += "\n" + row[0]
            
        with open(self.SnakeMake_FileName, 'w') as snakefile:
            snakefile.write(self.snakefile_content)
            
        print("Printed to " + self.SnakeMake_FileName)
        
        
if __name__ == "__main__":
    user_recording_files = ["data/raw/0012_rest.raw","data/raw/0274_rest.raw", "data/raw/0280_rest.raw", "data/raw/0418_rest.raw", "data/raw/1807_rest.raw", "data/raw/2502_rest.raw"]
    # Your DAG represented as a dictionary
    # user_dag_dict_MNE = {
    #     "MNE import raw EGI 128": {"MNE 1 Hz Highpass Filter"},
    #     "MNE 1 Hz Highpass Filter":{"MNE 80 Hz Lowpass Filter"},
    #     "MNE 80 Hz Lowpass Filter":{"MNE 57 Hz to 63 Hz Notch Filter"},
    #     "MNE 57 Hz to 63 Hz Notch Filter":{"MNE Resample to 250 Hz"},
    #     "MNE Resample to 250 Hz":{"MNE Rereference"},
    #     "MNE Rereference":{"MNE Channel Interpolation Spherical"},
    #     "MNE Channel Interpolation Spherical":{},
    # }
    # sc = SnakeControl(user_recording_files,user_dag_dict_MNE)
    
    user_dag_dict_VHTP = {
        "VHTP import raw EGI 128": {"VHTP 1 Hz Highpass Filter"},
        "VHTP 1 Hz Highpass Filter":{"VHTP 80 Hz Lowpass Filter"},
        "VHTP 80 Hz Lowpass Filter":{"VHTP 57 Hz to 63 Hz Notch Filter"},
        "VHTP 57 Hz to 63 Hz Notch Filter":{"VHTP Resample to 250 Hz"},
        "VHTP Resample to 250 Hz":{"VHTP Rereference"},
        "VHTP Rereference":{"VHTP Channel Interpolation Spherical"},
        "VHTP Channel Interpolation Spherical":{},
    }
    sc = SnakeControl(user_recording_files,user_dag_dict_VHTP)

    sc.run()
    
