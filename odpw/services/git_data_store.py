import codecs
import os

import yaml
import structlog
import sh
import requests


from odpw.core.api import DBClient
from odpw.core.model import Portal

from odpw.utils.utils_snapshot import getCurrentSnapshot


log = structlog.get_logger()


def csv_clean(datasetdir, csvengine):
    resources_dir = os.path.join(datasetdir, 'resources')
    clean_dir = os.path.join(datasetdir, 'cleaned')
    if not os.path.exists(clean_dir):
        os.mkdir(clean_dir)
    if os.path.exists(resources_dir):
        for r in os.listdir(resources_dir):
            r_path = os.path.join(resources_dir, r)
            with open(r_path, 'rb') as f:
                req = requests.post(csvengine, files={'csv_file': f})
                if req.status_code == 200:
                    with codecs.open(os.path.join(clean_dir, r), 'w', 'utf-8') as out_f:
                        out_f.write(req.content.decode('utf-8'))


def git_update(portal, snapshot, git_config, clean):
    log.info("START GIT UPDATE", portal=portal.id, snapshot=snapshot, git=git_config)

    p_dir = os.path.join(git_config['datadir'], portal.id)
    if not os.path.exists(p_dir):
        log.warn("NO DATA FOR PORTAL", portalid=portal.id, portaldata=p_dir)
        return

    # get groups and share with group
    groups_url = git_config['url'] + 'api/v4/groups'
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
                create_repo = git_config['url'] + 'api/v4/projects'
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

            # CSV clean
            if clean:
                csv_clean(datasetdir, git_config['csvengine'])

            # add untracked files
            log.debug("GIT STATUS", git=git.status())
            log.debug("GIT ADD", git=git.add('-A'))
            try:
                # commit
                log.debug("GIT COMMIT", git=git.commit(m=str(snapshot)))
                # pull any changes
                log.debug("GIT PULL", git=git.pull('origin', 'master'))
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
    pa.add_argument('--clean', help="Run the CSV clean service (if CSV file available)", action='store_true')


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
            git_update(P, sn, git, args.clean)
    else:
        for P in db.Session.query(Portal):
            git_update(P, sn, git, args.clean)
