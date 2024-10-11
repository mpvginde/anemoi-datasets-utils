import yaml
import os
from ecflow import *
from argparse import ArgumentParser

DEFAULT_CONFIG = dict(
    config=None,
    output=None,
    load_parts=4,
    load_tasks=4,
    max_load_tasks_running=4,
    additional_statistics=True,
    additional_statistics_parts=10,
    cleanup=True,
    patch=True,
    conda_environment=None,
)

DEFAULT_ECF = dict(
    ECF_TRIES=str(1),
    ECF_USER=os.getenv("USER"),
    ECF_HOME='{suite_path}/jobs',
    ECF_INCLUDE='{suite_path}/include',
    ECF_FILES='{suite_path}/scr',
    ECF_OUT=f'/scratch/{os.getenv("USER")}/ECF',
    ECF_LOGHOST='hpc-login',
    ECF_LOGPORT=str(os.getuid()+35000),
    ECF_JOB_CMD= '%TROIKA% -c %TROIKA_CONFIG% '\
                    'submit -o %ECF_JOBOUT% %HOST% %ECF_JOB%',
    ECF_KILL_CMD='%TROIKA% -c %TROIKA_CONFIG% '\
                    'kill %HOST% %ECF_JOB%',
    ECF_STATUS_CMD='%TROIKA% -c %TROIKA_CONFIG% '\
                    'monitor %HOST% %ECF_JOB%'
)
COMMANDS = dict(
    init="init",
    load="load",
    final="finalise",
    init_statistics="init-additions",
    load_statistics="load-additions",
    final_statistics="finalise-additions",
    cleanup="cleanup",
    patch="patch",
)


class Creator:
    def __init__(self,config_file,definition_file):
        self.config = self._load_config(config_file)
        self.definition_file = definition_file
        # cannot use path.abspath as this will give the real ecmwf path
        # /etc/... and does not work well with ecFLow
        self.suite_path = os.getenv("PWD")
        try:
            self.suite_name = os.path.basename(self.config["output"]).replace("-","_")
        except KeyError:
            raise KeyError("Please provide an output")
        self._set_conda()
        self._set_parts()
        self.output = self.config.get("output","dataset.zarr")
        self.config_file = self.config.get("config")

    def init_suite(self):
        """
        Initialises the suite by setting some suite-general variables:
            - ECF_*     : ecFlow variables
            - HOST      : server where the jobs will run
            - MODULES   : what modules to load e.g. (module load conda)
            - OUTPUT    : full path of the output .zarr file
            - CONFIG    : full path of the dataset .yaml recipe
        """
        self.defs = Defs()
        self.suite = self.defs.add_suite(self.suite_name)
        # ECF variables
        for key, value in DEFAULT_ECF.items():
            self.suite.add_variable(key,value.format(suite_path=self.suite_path))
        # conda command
        self.suite.add_variable("CONDA",self.conda)
        self.suite.add_variable("MODULES",self.modules)
        self.suite.add_variable("OUTPUT",self.output)
        self.suite.add_variable("CONFIG","")
        self.suite.add_variable("HOST","hpc")
 

    
    def define_suite(self):
        """
        Create the full definition of the suite
        """
        #TODO: a lot of stuff can be moved to specialized functions
        #TODO: implement max number of tasks running
        # Initialize
        family = self.suite.add_family("init")
        self._set_resources(family)
        family.add_variable("COMMAND",COMMANDS[family.name()])
        family.add_variable("CONFIG",self.config_file)
        family.add_variable("FLAGS",self._build_flags(family))
        family.add_task("run")


        # Load
        family = self.suite.add_family("load")
        family.add_trigger("init == complete")
        self._set_resources(family)
        family.add_variable("COMMAND",COMMANDS[family.name()])
        family.add_variable("FLAGS",self._build_flags(family))
        for i,parts in enumerate(self.parts):
            _family = family.add_family(f"loader_{i+1}")
            _family.add_repeat(RepeatString("PART",parts))
            _family.add_task("run")
        
        # Finalise
        family = self.suite.add_family("final")
        family.add_trigger("load == complete")
        self._set_resources(family)
        family.add_variable("COMMAND",COMMANDS[family.name()])
        family.add_variable("FLAGS",self._build_flags(family))
        family.add_task("run")

        
        # Additional statistics
        if self.config.get("statistics",False):
            stat = self.suite.add_family("additional_statistics")
            stat.add_trigger("final == complete")
            for hour in self.config.get("deltas",[24]):
                delta = stat.add_family(f"delta_{hour}h")
                delta.add_variable("DELTA",hour)
                # Init
                family = delta.add_family("init_statistics")
                self._set_resources(family)
                family.add_variable("COMMAND",COMMANDS[family.name()])
                family.add_variable("FLAGS",self._build_flags(family))
                family.add_task("run")

                # Load
                family = delta.add_family("load_statistics")
                family.add_trigger("init_statistics == complete")
                self._set_resources(family)
                family.add_variable("COMMAND",COMMANDS[family.name()])
                family.add_variable("FLAGS",self._build_flags(family))
                for i,parts in enumerate(self.statistics_parts):
                    _family = family.add_family(f"loader_{i+1}")
                    _family.add_repeat(RepeatString("PART",parts))
                    _family.add_task("run")
                
                #Final 
                family = delta.add_family("final_statistics")
                family.add_trigger("load_statistics == complete")
                self._set_resources(family)
                family.add_variable("COMMAND",COMMANDS[family.name()])
                family.add_variable("FLAGS",self._build_flags(family))
                family.add_task("run")

        if self.config.get("patch",False) or self.config.get("cleanup",False):
            post = self.suite.add_family("post")
            post.add_trigger("additional_statistics == complete")
            if self.config.get("patch",False):
                family = post.add_family("patch")
                self._set_resources(family)
                family.add_variable("COMMAND",COMMANDS[family.name()])
                family.add_variable("FLAGS",self._build_flags(family))
                family.add_task("run")
            if self.config.get("cleanup",False):
                family = post.add_family("cleanup")
                self._set_resources(family)
                family.add_variable("COMMAND",COMMANDS[family.name()])
                family.add_variable("FLAGS",self._build_flags(family))
                family.add_task("run")
      
    def run(self):
        """
        Create and save the definition file
        """
        self.init_suite()
        self.define_suite()
        self.defs.save_as_defs(self.definition_file)

    def _load_config(self,config_file):
        """
        Load the YAML config-file
        """
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        return config

    def _set_parts(self):
        """
        Helper function to organize the load parts among the ecFlow tasks
        """
        # Load
        nparts = self.config.get("load_parts",4)
        self.nparts = nparts
        ntasks = self.config.get("load_tasks",4)
        self.parts = [[] for _ in range(ntasks)]
        for i in range(nparts):
            self.parts[i%ntasks].append(str(i+1))
        
        # Statistics
        nparts = self.config.get("statistics_parts",4)
        self.statistics_nparts = nparts
        ntasks = self.config.get("statistics_tasks",4)
        self.statistics_parts = [[] for _ in range(ntasks)]
        for i in range(nparts):
            self.statistics_parts[i%ntasks].append(str(i+1))
        
    def _set_conda(self):
        """ 
        Helper function to define the conda-environment
        """
        conda = self.config.get("conda_environment",None)
        if conda:
            self.conda = f'conda run --live-stream -n {conda}'
            self.modules = 'module load conda'
        else:
            self.conda = ''
            self.modules = ''
        
    def _set_resources(self,node):
        """
        Helper function to define the correct compute resources
        to the different ecFlow nodes

        Parameters
        ----------
        node:   ecFlow.Node
                ecFlow node (suite,family,task) of which you want
                to set the resources
        """
        resources = self.config["resources"].get(node.name())
        for key,value in resources.items():
        #    print(key.upper())
            node.add_variable(key.upper(),value)
    
    def _build_flags(self,node):
        """
        Helper functions to build the correct flags for each 
        task / anemoi-datasets command
        
        Parameters
        ----------
        node:   ecFlow.Node
                ecFlow node for which you want to build the flags
        
        Returns
        -------
        str
                combination of all needed flags
        """

        if node.name() not in COMMANDS.keys():
            raise Exception("Building flag at the wrong level")
        
        cache = self.config.get("cache",None)
        if (
            node.name() == "init" or 
            node.name() == "final" or 
            node.name() == "cleanup " or
            node.name() == "patch"
        ):
            flag = ""
        
        elif node.name() == "load":
            flag = f"--part %PART%/{self.nparts}"
            if cache and node.name()=="load":
                flag += f" --cache {cache}"
        
        elif node.name() == "init_statistics" or node.name == "final_statistics":
            flag = "--delta %DELTA%h"
        
        elif node.name() == "load_statistics":
            flag = f"--part %PART%/{self.statistics_nparts}"
            flag += " --delta %DELTA%h"
        
        else:
            # In principle we should never end up here
            flag=""
        
        # Check for trace
        trace = self.config.get("trace",False)
        if trace and not (node.name()=="cleanup" or node.name()=="patch"):
            flag += " --trace"
        
        # Check for overwrite
        overwrite = self.config.get("overwrite",False)
        if overwrite and node.name()=="init":
            flag += " --overwrite"

        return flag
        
def main(): 
    parser = ArgumentParser(description="Build an ecFlow-suite for creating an anemoi-dataset incrementally.")
    parser.add_argument(
        '-d',
        '--definition',
        type=str,
        help='name of the generated definition-file (default: anemoi-dataset.def)',
        default="anemoi-dataset.def"
    )
    required = parser.add_argument_group('required arguments')
    required.add_argument(
        '-c',
        '--config',
        type=str,
        help='configuration yaml-file',
        required=True
    )
    args = parser.parse_args()
    creator = Creator(args.config,args.definition)
    creator.run()

if __name__ == "__main__":
    main()


