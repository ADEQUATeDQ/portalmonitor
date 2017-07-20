import os

import yaml
import structlog
import sh
import requests
import urlparse


from odpw.core.api import DBClient
from odpw.core.model import Portal

from odpw.utils.utils_snapshot import getCurrentSnapshot


log = structlog.get_logger()


def git_update(portal, snapshot, git_config):
    log.info("START GIT UPDATE", portal=portal.id, snapshot=snapshot, git=git_config)

    p_dir = os.path.join(git_config['datadir'], portal.id)
    if not os.path.exists(p_dir):
        log.warn("NO DATA FOR PORTAL", portalid=portal.id, portaldata=p_dir)
        return

    # get groups and share with group
    groups_url = urlparse.urljoin(git_config['url'], 'api/v4/groups')
    resp = requests.get(groups_url, headers={'PRIVATE-TOKEN': git_config['token']})
    groups = resp.json()

    group_id = None
    for g in groups:
        if g['name'] == portal.id:
            group_id = g['id']
            break

    for d_dir in os.listdir(p_dir):
        datasetdir = os.path.join(p_dir, d_dir)
        if os.path.isdir(datasetdir):
            # get git commands
            git = sh.git.bake(_cwd=datasetdir)
            if not os.path.exists(os.path.join(datasetdir, '.git')):
                # new remote repository
                create_repo = urlparse.urljoin(git_config['url'], 'api/v4/projects')
                args = {'path': d_dir, 'visibility': 'public', 'lfs_enabled': True}
                if group_id:
                    args['namespace_id'] = group_id

                resp = requests.post(create_repo, data=args, headers={'PRIVATE-TOKEN': git_config['token']})
                if resp.status_code == 201:
                    data = resp.json()
                    repo_id = data['id']
                    repo_ssh = data['ssh_url_to_repo']
                    log.info("Repository created", data=data)
                else:
                    log.warn("GIT API: could not create project", dataset=d_dir, response=resp.content)
                    continue

                # clone repo
                log.debug("GIT INIT", git=git.init())
                log.debug("GIT REMOTE", git=git.remote('add', 'origin', repo_ssh))
            else:
                # pull any changes
                log.debug("GIT PULL", git=git.pull('origin', 'master'))

            # add untracked files
            log.debug("GIT STATUS", git=git.status())
            log.debug("GIT ADD", git=git.add('.'))
            try:
                # commit
                log.debug("GIT COMMIT", git=git.commit(m=str(snapshot)))
                # git push origin master
                log.debug("GIT PUSH", git=git.push('origin', 'master'))
            except Exception as e:
                if 'nothing to commit, working directory clean' in e.message:
                    log.debug("NO CHANGES TO DATASET", dataset=d_dir, snapshot=snapshot, portal_id=portal.id)
                else:
                    raise e

def help():
    return "perform head lookups"


def name():
    return 'GitDataStore'


def setupCLI(pa):
    pa.add_argument('--pid', dest='portalid', help="Specific portal id")


def cli(args, dbm):
    db= DBClient(dbm)

    git = None
    if args.config:
        with open(args.config) as f_conf:
            config = yaml.load(f_conf)
            if 'git' in config:
                git = config['git']

    if not git:
        log.warn("GIT LOCATION OR URL NOT SPECIFIED")
        return

    sn = getCurrentSnapshot()


    if args.portalid:
        P =db.Session.query(Portal).filter(Portal.id==args.portalid).one()
        if P is None:
            log.warn("PORTAL NOT IN DB", portalid=args.portalid)
            return
        else:
            git_update(P, sn, git)
    else:
        for P in db.Session.query(Portal):
            git_update(P, sn, git)


