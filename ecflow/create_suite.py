import yaml
import os
import datetime
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
    # Job output on HOME: the ecFlow VM mounts HOME/PERM only (not SCRATCH/HPCPERM), and
    # ECMWF advises against PERM for ECF_OUT. A log server on the VM serves this directory
    # on port 35000+uid; ecflow_ui reaches it through the ssh tunnel, hence localhost.
    ECF_OUT=f'{os.getenv("HOME")}/ecflow_out',
    ECF_LOGHOST='localhost',
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


        # Prestage (optional): recall the MARS data of each month from tape before loading
        prestage = self.config.get("prestage")
        if prestage:
            self._define_prestage(prestage)

        # Load
        family = self.suite.add_family("load")
        family.add_trigger("init == complete")
        self._set_resources(family)
        family.add_variable("COMMAND",COMMANDS[family.name()])
        family.add_variable("FLAGS",self._build_flags(family))
        for i,parts in enumerate(self.parts):
            _family = family.add_family(f"loader_{i+1}")
            _family.add_repeat(RepeatString("PART",parts))
            task = _family.add_task("run")
            if prestage:
                task.add_trigger(self._prestage_trigger(_family, parts))
        
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

    def _define_prestage(self, prestage):
        """
        Add a family `prestage` with one family per month (`m<YYYY>_<MM>`), each running one
        MARS request file that covers that month. Loaders wait (see _prestage_trigger) for the
        months of the part they are about to load, so they find the data on disk instead of
        waiting for tape inside their walltime.
        """
        self._load_dates()
        months = sorted({(d.year, d.month) for d in self.dates})

        request_dir = os.path.join(self.suite_path, "requests", self.suite_name)
        os.makedirs(request_dir, exist_ok=True)

        self.suite.add_limit("prestage", int(prestage.get("max_running", 4)))
        family = self.suite.add_family("prestage")
        family.add_inlimit("prestage")
        self._set_resources(family)
        for year, month in months:
            in_month = [d for d in self.dates if (d.year, d.month) == (year, month)]
            path = os.path.join(request_dir, f"{year}_{month:02d}.req")
            with open(path, "w") as f:
                f.write(self._mars_requests(prestage, in_month[0], in_month[-1]))
            _family = family.add_family(self._month_name(year, month))
            _family.add_variable("REQUEST", path)
            _family.add_task("stage")

    def _mars_requests(self, prestage, first, last):
        """
        MARS request text for the dates first..last (inclusive). Each entry of
        prestage['requests'] is a MARS request without date; lists are joined with '/'.
        """
        verb = prestage.get("verb", "stage")
        if verb not in ("stage", "retrieve"):
            raise ValueError(f"prestage verb must be 'stage' or 'retrieve', not {verb}")
        text = []
        for template in prestage["requests"]:
            request = dict(template)
            request["date"] = f"{first:%Y%m%d}/to/{last:%Y%m%d}"
            if verb == "retrieve":
                request["target"] = '"/dev/null"'
            lines = [verb]
            for key, value in request.items():
                if isinstance(value, (list, tuple)):
                    value = "/".join(str(v) for v in value)
                lines.append(f"    {key}={value}")
            text.append(",\n".join(lines))
        return "\n\n".join(text) + "\n"

    def _prestage_trigger(self, loader, parts):
        """
        Trigger for loader/run: the prestage months of the part the loader's PART repeat is
        on are complete. For a RepeatString, `:PART` in a trigger is the index in the list.
        """
        loader_path = loader.get_abs_node_path()
        conditions = []
        for index, part in enumerate(parts):
            months = sorted({(d.year, d.month) for d in self._part_dates(int(part))})
            staged = " and ".join(
                f"/{self.suite_name}/prestage/{self._month_name(*m)} == complete" for m in months
            ) or "1 == 1"
            conditions.append(f"({loader_path}:PART == {index} and {staged})")
        return " or ".join(conditions)

    @staticmethod
    def _month_name(year, month):
        return f"m{year}_{month:02d}"

    def _load_dates(self):
        """
        Dates of the dataset from the anemoi-datasets recipe (`base_dates` for the
        trajectories layout, else `dates`), and the group size (`build.group_by`).
        Only start/end/frequency dates and an integer group_by are supported.
        """
        recipe = self._load_config(self.config_file)
        dates = recipe.get("base_dates", recipe.get("dates"))
        start, end = (self._as_datetime(dates[k]) for k in ("start", "end"))
        step = self._as_timedelta(dates.get("frequency", "1d"))
        self.dates = []
        while start <= end:
            self.dates.append(start)
            start += step
        self.group_by = recipe.get("build", {}).get("group_by")
        if not isinstance(self.group_by, int):
            raise ValueError(f"prestage needs an integer build.group_by, not {self.group_by!r}")

    def _part_dates(self, part):
        """
        Dates loaded by `--part part/nparts`, with the same split as
        anemoi-datasets create/parts.py (groups of group_by dates).
        """
        ngroups = -(-len(self.dates) // self.group_by)
        chunk = ngroups / self.nparts
        groups = [g for g in range(ngroups) if (part - 1) * chunk <= g < part * chunk]
        return [d for g in groups for d in self.dates[g * self.group_by:(g + 1) * self.group_by]]

    @staticmethod
    def _as_datetime(value):
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        return datetime.datetime.fromisoformat(str(value))

    @staticmethod
    def _as_timedelta(value):
        if isinstance(value, int):
            return datetime.timedelta(hours=value)
        value = str(value)
        units = {"h": "hours", "d": "days"}
        return datetime.timedelta(**{units[value[-1]]: int(value[:-1])})

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
                cache = os.path.join(cache,"tmp_%FAMILY1%")
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


