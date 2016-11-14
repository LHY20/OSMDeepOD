import argparse
import configparser
import logging
import logging.handlers
import os
from redis.exceptions import ConnectionError

from src.base.search import Search
from src.base.bbox import Bbox
from src.role.worker import Worker
from src.role.manager import Manager


def redis_args(config):
    return [config['REDIS']['server'], config['REDIS']['port'], config['REDIS']['password']]


def manager(args, config):
    big_bbox = Bbox.from_lbrt(
        args.bb_left,
        args.bb_bottom,
        args.bb_right,
        args.bb_top)
    try:
        print('Manger has started...')
        parameters = dict()
        parameters['word'] = config.get(section='DETECTION', option='Word', fallback='crosswalk')
        parameters['key'] = config.get(section='DETECTION', option='Key', fallback='highway')
        parameters['value'] = config.get(section='DETECTION', option='Value', fallback='crosswalk')
        parameters['zoom'] = config.getint(section='DETECTION', option='Zoom', fallback=19)
        parameters['compare'] = config.getboolean(section='DETECTION', option='Compare', fallback=True)
        parameters['orthofoto'] = config.get(section='DETECTION', option='Orthofoto', fallback='other')
        parameters['network'] = config.get(section='DETECTION', option='Network')
        parameters['labels'] = config.get(section='DETECTION', option='Labels')
        parameters['streets'] = config.getboolean(section='DETECTION', option='Streets', fallback=True)
        parameters['bbox_size'] = config.getint(section='JOB', option='BboxSize', fallback=2000)
        parameters['timeout'] = config.getint(section='JOB', option='Timeout', fallback=5400)
        search = Search(parameters=parameters)

        Manager.from_big_bbox(
            big_bbox,
            redis_args(config),
            'jobs',
            search)
    except ConnectionError:
        print(
            'Failed to connect to redis instance [{ip}:{port}], is it running? Check connection arguments and retry.'.format(
                ip=config['REDIS']['server'],
                port=config['REDIS']['port']))
    finally:
        print('Manager has finished!')


def job_worker(_, config):
    worker = Worker.from_worker(['jobs'])
    try:
        print('JobWorker has started...')
        worker.run(redis_args(config))
    except ConnectionError:
        print(
            'Failed to connect to redis instance [{ip}:{port}], is it running? Check connection arguments and retry.'.format(
                ip=config['REDIS']['server'],
                port=config['REDIS']['port']))
    finally:
        print('JobWorker has finished!')


def result_worker(_, config):
    worker = Worker.from_worker(['results'])
    try:
        print('ResultWorker has started...')
        worker.run(redis_args(config))
    except ConnectionError:
        print(
            'Failed to connect to redis instance [{ip}:{port}], is it running? Check connection arguments and retry.'.format(
                ip=config['REDIS']['server'],
                port=config['REDIS']['port']))
    finally:
        print('ResultWorker has finished!')


def set_logger():
    root_logger = logging.getLogger()
    syslog_handler = logging.handlers.SysLogHandler(address=('localhost', 514))
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s %(name)s')
    syslog_handler.setFormatter(formatter)
    root_logger.addHandler(syslog_handler)
    root_logger.setLevel(logging.WARNING)


def check_manager_config(config):
    if not config.has_section('DETECTION'): raise Exception(
        "Section 'DETECTION' is not in config file!")

    if not config.has_option('DETECTION', 'network'): raise Exception(
        "'network' not in 'DETECTION' section! ")
    network = config.get(section='DETECTION', option='network')
    if not os.path.isfile(network): raise Exception("The config file does not exist! " + network)

    labels = config.get(section='DETECTION', option='labels')
    if not config.has_option('DETECTION', 'labels'): raise Exception(
        "'labels' not in 'DETECTION' section! ")
    if not os.path.isfile(labels): raise Exception("The config file does not exist! " + labels)


def read_config(args):
    config_file = args.config
    config = configparser.ConfigParser()
    if not os.path.isfile(config_file): raise Exception("The config file does not exist! " + config_file)

    config.read(config_file)
    if not os.path.isfile(config_file): raise Exception("The config file could not be red! " + config_file)
    if not config.has_section('REDIS'): raise Exception("Section 'REDIS' is not in config file! " + config_file)
    if not config.has_option('REDIS', 'Server'): raise Exception("'server' no in 'REDIS' section! " + config_file)
    if not config.has_option('REDIS', 'Password'): raise Exception("'password' no in 'REDIS' section! " + config_file)
    if not config.has_option('REDIS', 'Port'): raise Exception("'port' no in 'REDIS' section! " + config_file)

    if args.role is 'manager':
        check_manager_config(config)
    return config


def mainfunc():
    set_logger()
    parser = argparse.ArgumentParser(description='Detect crosswalks.', )
    parser.add_argument(
        '-c',
        '--config',
        action='store',
        dest='config',
        required=True,
        help='The path to the configuration file.'
    )

    subparsers = parser.add_subparsers(
        title='worker roles',
        description='',
        dest='role',
        help='Select the role of this process'
    )

    subparsers.required = True

    p_manager = subparsers.add_parser(
        'manager',
        help='Splits up the given bounding box (WGS84, minlon/minlat/maxlon/maxlat) into small pieces and puts them into the redis queue to be consumed by the jobworkers.')
    p_manager.add_argument(
        'bb_left',
        type=float,
        action='store',
        help='left float value of the bounding box (WGS84, minlon)')
    p_manager.add_argument(
        'bb_bottom',
        type=float,
        action='store',
        help='bottom float value of the bounding box (WGS84, minlat)')
    p_manager.add_argument(
        'bb_right',
        type=float,
        action='store',
        help='right float value of the bounding box (WGS84, maxlon)')
    p_manager.add_argument(
        'bb_top',
        type=float,
        action='store',
        help='top float value of the bounding box (WGS84, maxlat)')
    p_manager.set_defaults(func=manager)

    p_jobworker = subparsers.add_parser(
        'jobworker',
        help='Detect crosswalks on element from the redis queue.')
    p_jobworker.set_defaults(func=job_worker)

    p_resultworker = subparsers.add_parser(
        'resultworker',
        help='Consolidate and write out results.')
    p_resultworker.set_defaults(func=result_worker)

    args = parser.parse_args()
    config = read_config(args)
    args.func(args, config)


if __name__ == "__main__":
    mainfunc()
