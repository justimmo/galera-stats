#!/usr/bin/python3
#
# Galera cluster node status logger
#

import argparse
import sys
import mysql.connector
import redis
import socket
import datetime
import json



##
# Fetches status information about the galera cluster
#
def fetch_stats(host, port, socket, username, password, timeout = 10, verbose = False):
    try:
        if socket:
            conn = mysql.connector.connect(
                unix_socket        = socket,
                user               = username,
                password           = password,
                connection_timeout = timeout,
            )
        else:
            conn = mysql.connector.connect(
                host               = host,
                port               = port,
                user               = username,
                password           = password,
                connection_timeout = timeout,
            )
    except mysql.connector.errors.InterfaceError as e:
        if verbose:
            print(e)
        sys.exit(1)

    cursor = conn.cursor()
    cursor.execute("SHOW GLOBAL STATUS LIKE 'wsrep_%';")

    stats = {}
    for (key, value) in cursor:
        stats[key] = value

    conn.close()
    return stats


##
# Puts a dictionary into logstash
#
def logstash(stats, host, port, db, rkey = 'logstash', verbose = False):
    conn = redis.StrictRedis(host, port, db)

    # we don't convert to floats because of possible conversion problems (trust mutate logstash here)
    logvars = {
        '@timestamp': datetime.datetime.utcnow().isoformat() + '+00:00',
        '@version':   1,
        'type':       'galera-cluster',
        'level':      'INFO',
        'host':       socket.gethostname(),
        'message':    'stats',

        # cluster stats
        'wsrep_cluster_state_uuid': stats['wsrep_cluster_state_uuid'],
        'wsrep_cluster_size':       int(stats['wsrep_cluster_size']),
        'wsrep_cluster_status':     stats['wsrep_cluster_status'],

        # node stats
        'wsrep_ready':                stats['wsrep_ready'],
        'wsrep_connected':            stats['wsrep_connected'],
        'wsrep_desync_count':         int(stats['wsrep_desync_count']),
        'wsrep_local_state':          int(stats['wsrep_local_state']),
        'wsrep_local_state_comment':  stats['wsrep_local_state_comment'],
        'wsrep_local_recv_queue_min': int(stats['wsrep_local_recv_queue_min']),
        'wsrep_local_recv_queue_max': int(stats['wsrep_local_recv_queue_max']),
        'wsrep_local_recv_queue_avg': stats['wsrep_local_recv_queue_avg'],
        'wsrep_local_send_queue_min': int(stats['wsrep_local_send_queue_min']),
        'wsrep_local_send_queue_max': int(stats['wsrep_local_send_queue_max']),
        'wsrep_local_send_queue_avg': stats['wsrep_local_send_queue_avg'],
        'wsrep_flow_control_paused':  stats['wsrep_flow_control_paused'],
    }

    jsonStr = json.dumps(logvars)

    try:
        conn.lpush('logstash', jsonStr)
    except redis.exceptions.ConnectionError as e:
        if verbose:
            print(e)
        sys.exit(1)

    if verbose:
        print(jsonStr)


##
# Parses CLI arguments
#
def get_arguments():
    parser = argparse.ArgumentParser(
        description     = 'Galera cluster node status logger',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument('--mysql-host',
        nargs   = 1,
        default = 'localhost',
        help    = 'MySQL hostname',
    )

    parser.add_argument('--mysql-port',
        nargs   = 1,
        type    = int,
        default = 3306,
        help    = 'MySQL port number',
    )

    parser.add_argument('--mysql-socket',
        nargs   = 1,
        default = None,
        help    = 'MySQL socket file (specify to not use hostname / port combination)',
    )

    parser.add_argument('--mysql-user',
        nargs   = 1,
        default = 'stats',
        help    = 'MySQL username',
    )

    parser.add_argument('--mysql-pass',
        nargs   = 1,
        default = 'stats',
        help    = 'MySQL password',
    )

    parser.add_argument('--redis-host',
        nargs   = 1,
        default = 'localhost',
        help    = 'Redis hostname',
    )

    parser.add_argument('--redis-port',
        nargs   = 1,
        type    = int,
        default = 6379,
        help    = 'Redis port number',
    )

    parser.add_argument('--redis-db',
        nargs   = 1,
        type    = int,
        default = 0,
        help    = 'Redis database',
    )

    parser.add_argument('--verbose',
        action  = 'store_true',
        help    = 'Print verbose output',
    )

    args = parser.parse_args()

    ret = {
        'mysql_user': args.mysql_user[0] if isinstance(args.mysql_user, list) else args.mysql_user,
        'mysql_pass': args.mysql_pass[0] if isinstance(args.mysql_pass, list) else args.mysql_pass,

        'redis_host': args.redis_host[0] if isinstance(args.redis_host, list) else args.redis_host,
        'redis_port': args.redis_port[0] if isinstance(args.redis_port, list) else args.redis_port,
        'redis_db':   args.redis_db[0]   if isinstance(args.redis_db, list)   else args.redis_db,

        'verbose': args.verbose,
    }

    if isinstance(args.mysql_socket, list):
        ret['mysql_socket'] = args.mysql_socket[0]
    else:
        ret['mysql_host'] = args.mysql_host[0] if isinstance(args.mysql_host, list) else args.mysql_host
        ret['mysql_port'] = args.mysql_port[0] if isinstance(args.mysql_port, list) else args.mysql_port


    return ret


##
# Main CLI run
#
def main():
    args = get_arguments()

    stats = fetch_stats(
        socket   = args['mysql_socket'] if 'mysql_socket' in args else None,
        host     = args['mysql_host']   if 'mysql_host' in args else None,
        port     = args['mysql_port']   if 'mysql_port' in args else None,
        username = args['mysql_user'],
        password = args['mysql_pass'],
        verbose  = args['verbose'],
    )

    logstash(stats,
        host    = args['redis_host'],
        port    = args['redis_port'],
        db      = args['redis_db'],
        verbose = args['verbose'],
    )


##
# Main
#
if __name__ == '__main__':
    main()
