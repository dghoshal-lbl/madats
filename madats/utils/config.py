"""
`madats.utils.config`
====================================

.. currentmodule:: madats.utils.config

:platform: Unix, Mac
:synopsis: Module defining Singleton configuration objects

.. moduleauthor:: Devarshi Ghoshal <dghoshal@lbl.gov>

"""

try:
    import configparser
except:
    from six.moves import configparser
import os
import sys

class Config():
    """
    Base configuration class
    """
    def __init__(self, config_file):
        self.__config_file__ = config_file
        
    def get(self, section, key):
        config = configparser.ConfigParser()
        config.read(self.__config_file__)
        value = str(config.get(section, key))
        return value


class PropertyConfig():
    """
    Configuration class defining the different properties of data
    """

    def __init__(self):
        if 'MADATS_HOME' in os.environ:
            pass
        else:
            print('MADATS_HOME is not set!')
            sys.exit()

        config_file = os.path.expandvars('$MADATS_HOME/config/config.cfg')
        #print('Reading config file: {}'.format(config_file))
        config = Config(config_file)
        self._short_term = config.get('persistence', 'shortterm')
        self._long_term = config.get('persistence', 'longterm')
        self._fixed_term = config.get('persistence', 'fixedterm')

    @property
    def SHORT_TERM(self):
        return self._short_term

    @property
    def LONG_TERM(self):
        return self._long_term

    @property
    def FIXED_TERM(self):
        return self._fixed_term

    @SHORT_TERM.setter
    def SHORT_TERM(self):
        self._short_term = config.get('persistence', 'shortterm')

    @LONG_TERM.setter
    def LONG_TERM(self):
        self._long_term = config.get('persistence', 'longterm')

    @FIXED_TERM.setter
    def FIXED_TERM(self):
        self._fixed_term = config.get('persistence', 'fixedterm')


class SchedulerConfig():
    """
    Class abstracting scheduler configurations: different options and commands in schedulers
    """

    def __init__(self, scheduler):
        config_dir = os.path.expandvars('$MADATS_HOME/config')
        config_file = scheduler+'.cfg'
        config_file = os.path.join(config_dir, config_file)
        #print('Reading config file: {}'.format(config_file))
        config = Config(config_file)

        self._submit = self._command_(config, 'submit')
        self._status = self._command_(config, 'status')

        self._prefix = config.get('directives', 'prefix')

        self._directives = {}
        self._directives['nodes'] = self._directive_(config, 'directives', 'nodes')
        self._directives['walltime'] = self._directive_(config, 'directives', 'walltime')
        self._directives['memory'] = self._directive_(config, 'directives', 'memory')
        self._directives['cpus'] = self._directive_(config, 'directives', 'cpus')
        self._directives['queue'] = self._directive_(config, 'directives', 'queue')
        self._directives['error'] = self._directive_(config, 'monitoring', 'error')
        self._directives['output'] = self._directive_(config, 'monitoring', 'output')
        self._directives['jobname'] = self._directive_(config, 'monitoring', 'jobname')
        self._directives['email'] = self._directive_(config, 'monitoring', 'email')

        self._emailopts = {}
        self._emailopts['begin'] = config.get('emailopts', 'begin')
        self._emailopts['end'] = config.get('emailopts', 'end')
        self._emailopts['fail'] = config.get('emailopts', 'fail')
        self._emailopts['all'] = config.get('emailopts', 'all')

    def _command_(self, config, _type):
        # TODO: check if the key is present; if not, assign default/NULL
        return config.get('commands', _type)

    def _directive_(self, config, section, _opt):
        directive = '#' + self._prefix + ' ' + config.get(section, _opt)
        return directive
        
    @property
    def submit(self):
        return self._submit

    @property
    def status(self):
        return self._status

    @property
    def directives(self):
        return self._directives

    @property
    def emailopts(self):
        return self._emailopts

# data property configuration object
property_config = PropertyConfig()

# scheduler configuration objects
slurm_config = SchedulerConfig('slurm')
pbs_config = SchedulerConfig('pbs')
