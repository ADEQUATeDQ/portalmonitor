import os

import yaml
import structlog
import sh
import requests


from odpw.core.api import DBClient
from odpw.core.model import Portal

from odpw.utils.utils_snapshot import getCurrentSnapshot

ADEQUATE_PORTALS_LOOKUP = {
    'data_gv_at': 'datagv',
    'www_opendataportal_at': 'opendataportal'
}


log = structlog.get_logger()

def get_readme_md(repo_name, portal_id):
    portal_name = ADEQUATE_PORTALS_LOOKUP[portal_id]
    ds_landing_page = "http://{0}.pages.adequate.at/{1}/".format(portal_name, repo_name)
    return "##### Click here to get to the ADEQUATe report and versions of this dataset:\n" \
           "#### [" + repo_name + "](" + ds_landing_page + ")"


def git_update(portal, snapshot, git_config, create_readme, max_file_size):
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

            # create README.md
            readme_file = os.path.join(datasetdir, 'README.md')
            if create_readme or not os.path.exists(readme_file):
                with open(readme_file, 'w') as f:
                    f.write(get_readme_md(d_dir, portal.id))

            # remove big files
            if max_file_size > 0:
                for root, dirs, files in os.walk(datasetdir):
                    for file_ in files:
                        res_path = os.path.join(root, file_)
                        if (os.path.getsize(res_path) >> 20) > max_file_size:
                            os.remove(res_path)

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
                    log.warn("GitDataStore - pull/push", dataset=d_dir, error=e.message)


def help():
    return "perform head lookups"


def name():
    return 'GitDataStore'


def setupCLI(pa):
    pa.add_argument('--pid', dest='portalid', help="Specific portal id")
    pa.add_argument('--readme', dest='readme', action='store_true', help="(Re-)create readme file for repositories.")
    pa.add_argument('--max-file-size', type=float, help="Set the maximum size for files in the repositories.", default=100)


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
            git_update(P, sn, git, create_readme=args.readme, max_file_size=args.max_file_size)
    else:
        for P in db.Session.query(Portal):
            git_update(P, sn, git, create_readme=args.readme, max_file_size=args.max_file_size)
