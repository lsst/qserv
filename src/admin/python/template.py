
import jinja2
import logging
import yaml


_log = logging.getLogger(__name__)


template_cfg_file_path = "/etc/qserv-template.cfg"


def save_template_cfg(values):
    """Save a dict of key-value pairs to the template config parameter file.

    Parameters
    ----------
    values : `dict` [`str`, `str`]
        Key-value pairs to add.
    """
    try:
        with open(template_cfg_file_path, "r") as f:
            cfg = yaml.safe_load(f.read())
    except FileNotFoundError:
        cfg = {}
    cfg.update(values)
    with open(template_cfg_file_path, "w") as f:
        f.write(yaml.dump(cfg))


def get_template_cfg():
    """Get the dit of key-value pairs from the config parameter file.
    """
    try:
        with open(template_cfg_file_path, "r") as f:
            cfg = yaml.safe_load(f.read())
    except FileNotFoundError:
        cfg = {}
    return cfg


def apply_template_cfg(template):
    """Apply template values as found in the config parameter file to a
    template.

    Parameters
    ----------
    template : `str`
        A template string with jinja-style templating.

    Returns
    -------
    result : `str`
        The result of having applied template values to the template.

    Raises
    ------
    ``jinja2.exceptions.UndefinedError``
        If any keys in the template are not found in the values.
    """
    template = jinja2.Template(
        "SELECT COUNT(*) FROM mysql.user WHERE user='{{ qserv_user }}' AND host='{{ host }}'",
        undefined=jinja2.StrictUndefined,
    )
    try:
        return template.render(**get_template_cfg())
    except jinja2.exceptions.UndefinedError as e:
        _log.error(f"Missing template value: {str(e)}")
        raise
