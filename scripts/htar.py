#!/bin/env python3

import sys
import argparse
import glob
from pathlib import Path
import re
import os
from functools import partial
from multiprocessing.dummy import Pool
from subprocess import call, run, check_output, STDOUT, PIPE
import shlex
import shutil
import time
import logging

class CustomFormatter(logging.Formatter):
    """Logging Formatter to add colors and count warning / errors"""
    grey = "\x1b[38;21m"
    bold = "\x1b[1m"
    yellow = "\x1b[33;21m"
    red = "\x1b[31;21m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(message)s"
    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: bold + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }
    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

logger = logging.getLogger("htar.py")

def scan_directory( root_dir ):
  # scans the directory for all files and their size in bytes
  for filename in glob.iglob(root_dir + '**/**', recursive=True):
     path = Path( filename )
     if not os.path.isdir(path): #path.is_dir():
       yield filename, path.stat().st_size

def split( root_dir, max_size=10000 ):
  # given a root_dir, yields counter, filename where the counter determines the archive number based of each archive being max_size
  n = 0
  acc = 0
  for filename, size in scan_directory( root_dir ):
    #logger.debug( f'{size}\t{acc} {max_size}\t{filename}' )
    acc += size
    if acc > max_size:
      n += 1
      acc = size
      yield n - 1, filename
    else:
      yield n, filename

def convert_to_bytes( size ):
  amount = int(re.sub("[^\d\.]", "", size))
  unit = re.sub("[\d\.]", "", size).lower()
  #logger.warn( f'amount: {amount} unit: {unit}' )
  if unit in ('k', 'kb'):
    amount *= 1024
  elif unit in ('m', 'mb'):
    amount *= 1024 * 1024
  elif unit in ('g', 'gb'):
    amount *= 1024 * 1024 * 1024
  elif unit in ('t', 'tb'):
    amount *= 1024 * 1024 * 1024 * 1024
  return amount

def create_file_lists( directory, max_size=1048576, prefix_path='', working_dir='/tmp/' ):
  #logger.info(f"Building archives for directory {directory} with archive sizes of {max_size}")
  file_lists = []
  logger.debug(f"organising files in {directory} into archives of size {max_size}")
  for archive_number, filename in split( str(directory), max_size=max_size ):

    # append the filename to the chunk
    filepath = filename.replace( prefix_path, '' ).replace(']', '\]').replace('[', '\[')
    #logger.debug( f'{archive_number}\t{filepath}' )

    try:
      file_lists[archive_number]['fh'].write( filepath + '\n' )
    except IndexError as e:
      #logger.warn(f"INDEX: {archive_number}")
      name = os.path.normpath(str(directory)).replace('/', ':')
      path = f'{working_dir}/htar_{name}.{archive_number}'
      logger.debug(f"filelist path {path}")
      # delete any old filelists
      try:
        os.remove( path )
      except FileNotFoundError as e:
        pass
      f = open( path, 'a' )
      if len(file_lists) == archive_number: # not zero index
        file_lists.append( { 'filelist': path, 'path': prefix_path, 'archive_number': archive_number, 'fh': open( path, 'a' ) } )
      # dont forget to write first line!
      file_lists[archive_number]['fh'].write( filepath + '\n' )

  # close files!
  for f in file_lists:
    f['fh'].close()

  return [ { 'path': f['path'], 'filelist': f['filelist'], 'archive_number': f['archive_number'] } for f in file_lists ]

def htar_command( directory, archive, file_list, htar_path='htar', hsi_prefix='/cryoEM/', archive_cos=110, index_cos=110 ):
  archive_path = Path( os.path.normpath(f"{hsi_prefix}/{directory}/{archive}") )
  log = f'{file_list}.out'
  cmd = f"""
echo \$ cd {directory} > {log}
echo \$ {htar_path} -Hcrc -Hnoglob -p -cvf {archive_path} -L {file_list}  -Y {archive_cos}:{index_cos} >> {log}
cd {directory} && {htar_path} -Hcrc -Hnoglob -p -cvf {archive_path} -L {file_list} -Y {archive_cos}:{index_cos} >> {log}
echo \$ {htar_path} -tv -f {archive_path} >> {log}
sleep 3
{htar_path} -tv -f {archive_path} >> {log}
"""
  #logger.info(f"+ {cmd}")
  return cmd, Path(f'{log}')

def hsi_create_directory( path, hsi_path='hsi', hsi_prefix='/cryoEM/exp', dry_run=True ):
  directory = os.path.normpath(f'{hsi_prefix}/{path}')
  logger.info(f"Creating parent directories at {directory}")
  cmd = f"{hsi_path} mkdir {directory}"
  if dry_run:
    logger.debug(f"Not executing: {cmd}")
  else:
    return run( cmd.split(), check=True )
  return

def is_exp_directory( path ):
  name = os.path.basename(os.path.normpath( path ))
  if name.startswith('20') and '-C' in name:
    return True
  return False

def create_stub_htar_extract_script( script_path, file_lists, directory, folder, htar_path="htar", hsi_prefix='/cryoEM/', dry_run=True, gather_data=False):
  header = '#' * 80 + '\n'
  header += """# The files under %s%s HAVE NOT been archived to tape. This is a stub generated by --gather_data option.
"""
  header += '#' * 80 + '\n'
  header += '\n'
  header += "# The following commands needs to be ran to extract the archives from tape back to disk\n"

  text = header % ( directory, folder )
  logger.warning( "Creating restore script %s" % (script_path,))
  for d in file_lists:
    archive = Path( f"{hsi_prefix}/{directory}/{folder}.{d['archive_number']}.tar" ) cmd = f"{htar_path} -xv -f {os.path.normpath(archive)}" text += f"{cmd}\n"
  text += "\n"
  text += '#' * 80 + '\n'
  with open( script_path, 'w' ) as f:
    f.write( text )


def create_htar_extract_script( script_path, file_lists, directory, folder, htar_path="htar", hsi_prefix='/cryoEM/', dry_run=True ):
  header = '#' * 80 + '\n'
  header += """# The files under %s%s has been archived to tape.
# To restore, please send email to unix-admin@slac.stanford.edu with the full directory path to this file and ask them to execute it.
"""
  header += '#' * 80 + '\n'
  header += '\n'
  header += "# The following commands needs to be ran to extract the archives from tape back to disk\n"

  text = header % ( directory, folder )
  logger.warning( "Creating restore script %s" % (script_path,))
  for d in file_lists:
    archive = Path( f"{hsi_prefix}/{directory}/{folder}.{d['archive_number']}.tar" )
    cmd = f"{htar_path} -xv -f {os.path.normpath(archive)}"
    text += f"{cmd}\n"
  text += "\n"
  text += '#' * 80 + '\n'
  if not dry_run:
    with open( script_path, 'w' ) as f:
      f.write( text )


def validate_archive( extract_script, folder_path, archive_path, cache=None ):
  if extract_script.exists() and cache == None:
    # logger.debug(f"Archive stub already exists {extract_script}...")
    logger.debug(f"reading extract script {extract_script}")
    cache = open( extract_script, 'r' ).read()
  else:
    logger.debug(f"using cached content for extract script {extract_script}")

  logger.info(f"Validating archive {archive_path}...")
  if not extract_script.exists():
    logger.warning(f"Archive log {extract_script} does not exist!")
    return False
  logger.debug(f"checking hpss for {archive_path}")
  # ensure it was logged as completed create succesffully
  archived_sizes = re.findall(f"Create complete for {archive_path}\. (\d+) bytes written for.*\n\#HTAR: HTAR SUCCESSFUL", cache, re.M)
  if len(archived_sizes) == 1:
    logger.debug(f"archive {archive_path} was logged as successfully archived with size {archived_sizes[0]}")
    if folder_path.exists():
      # ensure it exists on hpss
      cmd = f"hsi ls -l {archive_path}"
      logger.debug(f"archive reported as uploaded, checking archive on hpss using: {cmd}")
      hsi = run( cmd.split() ) #stdout=PIPE, stderr=PIPE )
      if not hsi.returncode == 0:
        logger.warn(f"HSI reports: {hsi}")
        raise SyntaxError(f"HSI did not report archive {archive_path} exists")
      logger.debug(f"hpss reports archive {archive_path} exists: TODO check size")

      # TODO, cant' capture output for some reason
      #logger.warn(f"HSI OUT: {hsi.stdout}")
      #hsi_output = re.findall(f"\s+(\d+) \w+ \d+\s+\d+\:\d+ (.*)$", f"{hsi.stdout}")
      #raise Exception(f"hsi reports {archive_path} does not exist")
      # check archive size

      logger.debug(f"archive {archive_path} was logged as succesfully tested")
      test = re.findall(f"Listing complete for {archive_path}, (\d+) files .*\n\#HTAR: HTAR SUCCESSFUL", cache, re.M)
      if len(test) == 1:
        return True
      else:
        SyntaxError(f"Failed htar test for archive {archive_path} defined in extract script {extract_script}")

      return False
  # we shouldn't have more than one entry
  elif len(archived_sizes) == 0:
    return False
  else:
    raise SyntaxError(f"Reported archive creates for {archive_path} defined in extract script {extract_script} ({len(archived_sizes)} entries)")

  return False

def delete_folder( folder_path, dry_run=True ):
  try:
    logger.warning(f"{'Should be ' if dry_run else ''}Deleting {folder_path}...")
    if not dry_run:
      shutil.rmtree( f"{folder_path}" )
  except Exception as e:
    logger.error(f"Could not delete {folder_path}: {e}")

def setup_folder( sample_path, folder, prefix='', archive_size=100*1024*1024*1024, hsi_prefix='/', dry_run=True, purge=False, gather_data=False ):

  folder_path = Path( f'{sample_path}/{folder}' )
  extract_script = Path( f'{folder_path}.htar' )

  # check to see if the htar extract file exists
  validate = None
  if extract_script.exists():
    #logger.warning(f"Archive stub already exists {extract_script}...")
    validate = open( extract_script, 'r' ).read()

  logger.info(f"Generating filelists for {sample_path} folder {folder}...")
  prefix = f'{os.path.normpath(sample_path)}/'
  file_lists = create_file_lists( folder_path, prefix_path=prefix, max_size=archive_size )

  # do not overwrite
  do_it = True
  if extract_script.exists():
    logger.warn(f"Existing extract script {extract_script} present...")
    if purge:
      do_it = True
    else:
      do_it = False

  do_it = not dry_run and do_it
  if do_it:
    create_htar_extract_script( extract_script, file_lists, prefix, folder, hsi_prefix=hsi_prefix, dry_run=dry_run )

  # new case: generate_data
  if gather_data:
    extract_script = Path( f'{folder_path}_STUB.htar' ) # temporarily add stub suffix
    create_stub_htar_extract_script( extract_script, file_lists, prefix, folder, hsi_prefix=hsi_prefix, dry_run=dry_run, gather_data=gather_data )
    extract_script = Path( f'{folder_path}.htar' ) # revert

  for d in file_lists:
    basename = os.path.basename(os.path.normpath(d['path']))
    archive = f"{folder}.{d['archive_number']}.tar"
    path = d['path']
    archive_path = f"{hsi_prefix}{path}{archive}"
    ok = None
    if extract_script.exists():
      ok = validate_archive( extract_script, folder_path, archive_path )
      if purge:
        ok = None
      logger.info(f"Archive {archive_path} previous status {ok} {'(purge)' if purge else ''}")
    logger.info(f"Preparing to archive {path} folder {folder} to {archive} with filelist {d['filelist']}, previous status {ok}")
    cmd, log = htar_command( path, archive, d['filelist'], hsi_prefix=hsi_prefix, archive_cos=args.archive_cos, index_cos=args.index_cos )
    yield { 'commands': cmd, 'log': log, 'extract_script': extract_script, 'filelist': d['filelist'], 'directory': folder_path, 'archive': archive, 'archive_path': archive_path, 'exists_okay': ok }

  return


def archive_folder( kwargs, dry_run=True ):
  extract_script=kwargs['extract_script']
  filelist=kwargs['filelist']
  directory=kwargs['directory']
  commands=kwargs['commands']
  archive=kwargs['archive']
  log=kwargs['log']
  duration = 0
  if dry_run:
    logger.error(f"Not archiving folder {directory}, archive {archive} from {filelist}, log {log} -- use --force to actually perform archive")
  else:
    logger.info(f"Archiving folder {directory}, archive {archive} from {filelist}, log {log}")
    try:
      #logger.debug(f"Running {commands}")
      start_time = time.monotonic()
      logger.debug(f"running {commands}")
      call(f"{commands}", shell=True)
      duration = (time.monotonic() - start_time)/60
      #logger.debug(f"Finished writing {archive} in {duration} minutes")
      # append this log to the extract script log
      if log.exists():
        logger.debug(f"Appending archive logs for {archive} to {extract_script}")
        with open( extract_script, 'a' ) as l:
          with open( log, 'r' ) as f:
            for i in f.readlines():
              l.write( f'#{i}' )
            l.write('#' * 80 + '\n')
        os.unlink( log )
        os.unlink( filelist )
    except Exception as e:
      logger.error(f"Archive {archive} for {directory} failed: {e}")
      raise e

  logger.info(f"Completed archiving partial folder {directory}, archive {archive} in {str(int(duration))+' minutes'}")

  return None if dry_run else True


def scan_folder( directory_folder, archive_size=100*1024*1024*1024, hsi_prefix='', dry_run=True, purge=False, gather_data=False ):
  logger.info(f"Analysing folder {directory_folder}")
  folder = os.path.basename(directory_folder)
  path = os.path.normpath( directory_folder ).replace(folder,'')
  for cmd in setup_folder( str(path), str(folder), archive_size=archive_size, prefix=path, hsi_prefix=hsi_prefix, dry_run=dry_run, purge=purge, gather_data=gather_data):
    yield cmd
  return

def scan_experiment( experiment_folder, archive_size=100*1024*1024*1024, hsi_prefix='', dry_run=True, purge=False, gather_data=False):
  logger.info(f"Found experimental folder {directory_path}")
  # assume sample directories underneath
  for sample in [x.name for x in directory_path.iterdir() if x.is_dir() and not x.is_symlink()]:
    sample_path = Path( str(directory_path) + '/' + sample )
    logger.info(f"Found sample folder {sample_path}")
    for folder in [x.name for x in sample_path.iterdir() if x.is_dir() and not x.is_symlink()]:
      path = os.path.normpath( f'{directory_path}/{sample}/{folder}' )
      prefix = f'{str(sample_path)}/'.replace('//','/')
      logger.info(f"Found folder {path} (prefix {prefix}")
      #for cmd in setup_folder( str(sample_path), str(folder), prefix=prefix, hsi_prefix=args.hsi_prefix, dry_run= not args.force ):
      for cmd in scan_folder( path, archive_size=archive_size, hsi_prefix=args.hsi_prefix, dry_run=dry_run, purge=purge, gather_data=gather_data ):
        yield cmd
  return




if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Will create n number of htar archives for a directory of specified size each.' )
  parser.add_argument('directory', nargs='+', help='directories to include in htar archives')
  parser.add_argument('--size', type=str, help='size in bytes of each archive', default='100g' )
  parser.add_argument('--hsi_prefix', type=str, help='hsi prefix path to place archives', default='/cryoEM/exp/' )
  parser.add_argument('--force', '-f', help='Commit all changes on disk and tape', default=False, action='store_true' )
  parser.add_argument('--really_force', '-F', help='Overwrite any previous archive scripts', default=False, action='store_true' )
  parser.add_argument('--threads', help='Number of concurrent htars to run', default=4, type=int )
  parser.add_argument('--no_relative_paths', help='Do not use relative paths from cwd', default=False, action='store_true' )
  parser.add_argument('--do_not_delete', help='Do not delete local files after archiving', default=False, action='store_true' )
  parser.add_argument('--archive_cos', help='set HPSS Class of Service (COS) for archive', default=110  )
  parser.add_argument('--index_cos', help='set HPSS Class of Service (COS) for index file', default=110  )
  parser.add_argument('--verbose', help='Debug output', default=False, action='store_true' )
  parser.add_argument('--gather_data', help='Collect usage data without write to HPSS', default=False, action='store_true' )

  args = parser.parse_args()

  if args.really_force == True:
    args.force = True

  # do not write or delete if only gathering data usage
  if args.gather_data == True:
    args.force = False
    args.really_force = False
    args.do_not_delete = True

  lvl = logging.INFO
  if args.verbose:
    lvl = logging.DEBUG
  logger.setLevel(lvl)
  ch = logging.StreamHandler()
  ch.setLevel(lvl)
  ch.setFormatter(CustomFormatter())
  logger.addHandler(ch)

  archive_size = convert_to_bytes( args.size )

  commands = []

  for directory in args.directory:

    if args.no_relative_paths or directory.startswith('/'):
      raise NotImplementedError("no relative paths not yet supported")

    directory_path = Path( directory )

    # 1) if given the path to an entire experimnet
    if is_exp_directory( directory_path ):
      for cmd in scan_experiment( directory_path, archive_size=archive_size, hsi_prefix=args.hsi_prefix, dry_run=not args.force, purge=args.really_force, gather_data=args.gather_data):
        commands.append( cmd )

    # 2) just push this folder to tape
    else:
      for cmd in scan_folder( directory_path, archive_size=archive_size, hsi_prefix=args.hsi_prefix, dry_run=not args.force, purge=args.really_force ):
        commands.append( cmd )

  # create the directory path in hpss
  precreate_dirs=[]
  for cmd in commands:
    d = f"{cmd['directory'].parent}"
    if not d in precreate_dirs:
      precreate_dirs.append(d)
  for d in precreate_dirs:
    relative = ''
    components = str(d).split('/')
    for c in components:
      relative = relative + '/' + c
      hsi_create_directory( relative, hsi_prefix=args.hsi_prefix, dry_run=not args.force )

  #logger.warn(f'{commands}')
  # filter out archives that are fine
  execute = [ c for c in commands if not c['exists_okay'] ]
  #sys.exit(127)

  if len(execute) == 0:
    logger.warn("No archive actions required")

  # actually run it! in parallel!
  failed = False
  pool = Pool(args.threads) # two concurrent commands at a time
  for i, returncode in enumerate( pool.imap( partial(archive_folder, dry_run=not args.force), execute) ):
    logger.warn(f"{i} of {len(execute)-1} returns {returncode}")
    if not args.force and returncode:
       logger.error(f"{i} command failed ({returncode}): {execute[i]}")
       failed = True

  #logger.warn(f"COMMANDS: {commands}")

  # delete folders if they've transfered okay
  # 1) case where it all uploaded prior
  if len(execute) == 0 and not is_exp_directory( directory ):
    logger.error(f"ABOUT TO DELETE {directory}")
    # remove dry_rund
    delete_folder( directory, dry_run=not args.force )

  # 2) when we did some uploading
  elif not failed:
    # reformat all archvies for each directory
    directories = {}
    for this in commands:
      if not this['directory'] in directories:
        directories[ this['directory'] ] = { 'extract_script': this['extract_script'], 'archives': [] }
      directories[ this['directory'] ]['archives'].append( this['archive_path'] )

    for directory, d in directories.items():
      res = []
      for archive in d['archives']:
        res.append( validate_archive( d['extract_script'], directory, archive ) )
      ok = len( [ x for x in res if x == True ] )
      logger.info(f"RES: {directory} {ok} / {len(res)}")
      # okay to delete directory!
      if False in res:
        logger.error(f"Archive validation of {directory} failed!")
      elif not args.force:
        logger.error(f"Dry run... would be deleting {directory}")
        delete_folder( directory, dry_run=True )
      elif not False in res:
        # remove dry_rund
        delete = not args.do_not_delete and args.force
        logger.error(f"DELETE? {delete} {directory}")
        delete_folder( directory, dry_run=not delete )
      else:
        if args.force:
          logger.error(f"Not deleting directory {directory} due to failed archive!")

  # 3) failed somehow
  else:
    logger.error("SOMETHING FAILED...")
