"""Sphinx configurations for pipeline_lsst_io.
These configurations are centrally defined in Documenteer
(https://github.com/lsst-sqre/documenteer).
"""

from documenteer.sphinxconfig.stackconf import \
    build_pipelines_lsst_io_configs


globals().update(build_pipelines_lsst_io_configs(
    project_name='Qserv',
))
