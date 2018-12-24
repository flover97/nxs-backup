#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import os
import json
import fnmatch
import tarfile
import math

import general_function
import general_files_func
import config
import log_and_mail
import mount_fuse
import specific_function


def inc_files_backup(job_data):
    ''' The function collects an incremental backup for the specified partition.

    '''

    try:
        job_name = job_data['job']
        sources = job_data['sources']
        storages = job_data['storages']
        rotation = job_data['rotation']
    except KeyError as e:
        log_and_mail.writelog('ERROR', "Missing required key:'%s'!" %(e),
                              config.filelog_fd, job_name)
        return 1
    
    for i in range(len(sources)):
        target_list = sources[i]['target']
        exclude_list = sources[i].get('excludes', '')
        gzip =  sources[i]['gzip']

        # Keeping an exception list in the global variable due to the specificity of
        # the `filter` key of the `add` method of the `tarfile` class
        general_files_func.EXCLUDE_FILES = general_files_func.get_exclude_ofs(target_list,
                                                                              exclude_list)

        # The backup name is selected depending on the particular glob patterns from
        # the list `target_list`
        for regex in target_list:
            target_ofs_list = general_files_func.get_ofs(regex)

            for i in target_ofs_list:
                if not general_files_func.is_excluded_ofs(i):
                    # Create a backup only if the directory is not in the exception list
                    # so as not to generate empty backups

                    # A function that by regularity returns the name of 
                    # the backup WITHOUT EXTENSION AND DATE
                    backup_file_name = general_files_func.get_name_files_backup(regex, i)

                    # Get the part of the backup storage path for this archive relative to
                    # the backup dir
                    part_of_dir_path = backup_file_name.replace('___', '/')

                    for j in range(len(storages)):
                        if specific_function.is_save_to_storage(job_name, storages[j]):
                            try:
                                current_storage_data = mount_fuse.get_storage_data(job_name,
                                                                                   storages[j])
                            except general_function.MyError as err:
                                log_and_mail.writelog('ERROR', '%s' %(err),
                                                      config.filelog_fd, job_name)
                                continue
                            else:
                                storage = current_storage_data['storage']
                                backup_dir = current_storage_data['backup_dir']
                                # Если storage активный - монтируем его
                                try:
                                    mount_fuse.mount(current_storage_data)
                                except general_function.MyError as err:
                                    log_and_mail.writelog('ERROR', "Can't mount remote '%s' storage :%s" %(storage, err),
                                                          config.filelog_fd, job_name)
                                    continue
                                else:
                                    remote_dir = ''  # Only for logging
                                    if storage != 'local':
                                        local_dst_dirname = mount_fuse.mount_point
                                        remote_dir = backup_dir
                                        if storage != 's3':
                                            host = current_storage_data['host']
                                        else:
                                            host = ''
                                        share =  current_storage_data.get('share')
                                    else:
                                        host = ''
                                        share = ''
                                        local_dst_dirname = backup_dir
                                    # We collect an incremental copy
                                    # For storage: local, sshfs, nfs backup_dir is the mount point and must already be created before mounting.
                                    # For storage: ftp, smb, webdav, s3 is NOT a mount point, but actually a relative path relative to the mount point
                                    if not storage in ('local', 'scp', 'nfs'):
                                        local_dst_dirname = os.path.join(local_dst_dirname, backup_dir.lstrip('/'))

                                    create_inc_file(local_dst_dirname, remote_dir, part_of_dir_path, backup_file_name, i, exclude_list, gzip, job_name, storage, host, share, rotation) #general_inc_iteration

                                    try:
                                        mount_fuse.unmount()
                                    except general_function.MyError as err:
                                        log_and_mail.writelog('ERROR', "Can't umount remote '%s' storage :%s" %(storage, err),
                                                              config.filelog_fd, job_name)
                                        continue
                        else:
                            continue
                else:
                    continue

def del_old_dirs(dirs,cnt):
    if len(dirs) > cnt :
        oldest_dir = min(dirs, key=os.path.getmtime)
        general_function.del_file_objects('inc_files', oldest_dir)
        dirs.remove(oldest_dir)
        del_old_dirs(dirs,cnt)
    return True

def create_inc_file(local_dst_dirname, remote_dir, part_of_dir_path, backup_file_name,
                    target, exclude_list, gzip, job_name, storage, host, share, rotation):
    ''' The function determines whether to collect a full backup or incremental,
    prepares all the necessary information.

    '''
    period = rotation['period']
    count = rotation['count']
    recreate_interval = rotation['recreate_interval']

    date_year = general_function.get_time_now('year')
    date_month = general_function.get_time_now('moy')
    date_day_of_month = general_function.get_time_now('dom')
    date_day_of_year = general_function.get_time_now('doy')
    date_period = general_function.get_time_now('period')

    year_dir = os.path.join(local_dst_dirname, part_of_dir_path, date_year)
    month_dir = os.path.join(year_dir, 'month_%s' %(date_month), 'monthly')
    top_period_dir = os.path.join(year_dir, 'full')

    if period == 'day':
        daily_dir = os.path.join(year_dir, 'day_%s'%(date_day_of_year))
        current_backup_dir = daily_dir # Contains the name of the backup directory that is currently being assembled, it is necessary for the correct rotation
    else:
        current_backup_dir = os.path.join(year_dir, 'month_%s' %(date_month))
        if int(date_day_of_month) < 11:
            daily_dir = os.path.join(year_dir, 'month_%s' %(date_month), 'daily', 'day_01')
        elif int(date_day_of_month) < 21:
            daily_dir = os.path.join(year_dir, 'month_%s' %(date_month), 'daily', 'day_11')
        else:
            daily_dir = os.path.join(year_dir, 'month_%s' %(date_month), 'daily', 'day_21')

    if os.path.isdir(top_period_dir):
        period_count = math.ceil(count/recreate_interval) # Variable to control the number of full copies
        backups_dirs = [os.path.join(year_dir,x) for x in os.listdir(year_dir) if os.path.join(year_dir, x) != top_period_dir] # All backup directories(2018/day_ or 2018/month_)
        period_dirs = [os.path.join(top_period_dir,d) for d in os.listdir(top_period_dir)] # All directories with full copies
        period_dirs.sort(key=os.path.getctime)
        last_period_dir_ctime = os.path.getctime(period_dirs[-1]) # Last full backup time
        backup_dirs_older_last_period_dir = [x for x in backups_dirs if os.path.getctime(x) >= last_period_dir_ctime] # All directories with full copies that were created after half a day full copies

        try :
            oldest_period_dir_ctime = os.path.getctime(period_dirs[-period_count]) # The time to create the oldest full copy, the copy is defined as the total number of full copies -perido_count
        except:
            oldest_period_dir_ctime = os.path.getctime(period_dirs[-1]) # Since the original copy with the index (-period_count) may not be, the oldest full copy is taken as the basis

        backup_dirs_older_oldest_period_dir = [x for x in backups_dirs if os.path.getctime(x) < oldest_period_dir_ctime] # All backup directories that were created before the oldest_period_dir_ctime
        

        if current_backup_dir not in backup_dirs_older_last_period_dir: # If current_backup_dir is not in the list backup_dirs_older_last_period_dir, then current_backup_dir is added there, for more correct rotation
            backup_dirs_older_last_period_dir.append(current_backup_dir)
            count -= 1 # Due to the fact that the current directory, which has not yet been created, has already been added to the list, it is necessary to lower the count for correct rotation

        if len(backup_dirs_older_last_period_dir) > recreate_interval:
            initial_dir = os.path.join(year_dir, top_period_dir, 'period_begins_from-%s' %(date_period))
            del_old_dirs(backups_dirs,count)
            if len(backup_dirs_older_oldest_period_dir) == 0 : # If the number of backups older than the oldest_period_dir_ctime is 0, then you can cause the deletion of the oldest full copy
                del_old_dirs(period_dirs,period_count)
        else:
            initial_dir = max(period_dirs, key=os.path.getmtime)
            del_old_dirs(backups_dirs,count)
            if len(backup_dirs_older_oldest_period_dir) == 0 : # If the number of backups older than the oldest_period_dir_ctime is 0, then you can cause the deletion of the oldest full copy
                del_old_dirs(period_dirs,period_count)
    else:
        initial_dir = os.path.join(year_dir, top_period_dir, 'period_begins_from-%s' %(date_period))
         
    full_inc_file  =  os.path.join(initial_dir, 'full.inc')
    month_inc_file =  os.path.join(month_dir, 'month.inc')
    daily_inc_file =  os.path.join(daily_dir, 'daily.inc')

    link_dict = {}  # dict for symlink with pairs like dst: src
    copy_dict = {}  # dict for copy with pairs like dst: src

    # Before we proceed to collect a copy, we need to delete the copies for the same month last year
    # if they are to not save extra archives

    if not os.path.isfile(full_inc_file):
        # There is no original index file, so we need to check the existence of an year directory
        if os.path.isdir(initial_dir):
            # There is a directory, but there is no file itself, then something went wrong, so
            # we delete this directory with all the data inside, because even if they are there
            # continue to collect incremental copies it will not be able to
            general_function.del_file_objects(job_name, initial_dir)
            dirs_for_log = general_function.get_dirs_for_log(initial_dir, remote_dir, storage)
            file_for_log = os.path.join(dirs_for_log, os.path.basename(full_inc_file))
            log_and_mail.writelog('ERROR', "The file %s not found, so the directory %s is cleared." +\
                                  "Incremental backup will be reinitialized " %(file_for_log, dirs_for_log), 
                                  config.filelog_fd, job_name)

        # Initialize the incremental backup, i.e. collect a full copy
        dirs_for_log = general_function.get_dirs_for_log(initial_dir, remote_dir, storage)
        general_function.create_dirs(job_name=job_name, dirs_pairs={initial_dir:dirs_for_log})

        # Get the current list of files and write to the year inc file
        meta_info = get_index(target, exclude_list)
        with open(full_inc_file, "w") as index_file:
            json.dump(meta_info, index_file)

        full_backup_path = general_function.get_full_path(  initial_dir,
                                                            backup_file_name, 
                                                            'tar',
                                                            gzip)

        general_files_func.create_tar('files', full_backup_path, target,
                                      gzip, 'inc_files', job_name,
                                      remote_dir, storage, host, share)

        # After creating the full copy, you need to make the symlinks for the inc.file and
        # the most collected copy in the month directory of the current month
        # as well as in the decade directory if it's local, scp the repository and
        # copy inc.file for other types of repositories that do not support symlynk.

        if period == 'day':
            daily_dirs_for_log = general_function.get_dirs_for_log(daily_dir, remote_dir, storage)
            general_function.create_dirs(job_name=job_name, dirs_pairs={daily_dir:daily_dirs_for_log})
        else:
            month_dirs_for_log = general_function.get_dirs_for_log(month_dir, remote_dir, storage)
            daily_dirs_for_log = general_function.get_dirs_for_log(daily_dir, remote_dir, storage)
            general_function.create_dirs(job_name=job_name, dirs_pairs={month_dir:month_dirs_for_log,
                                                                    daily_dir:daily_dirs_for_log})

        if storage in 'local, scp':
            if period == 'day':
                link_dict[daily_inc_file] = full_inc_file
                link_dict[os.path.join(daily_dir, os.path.basename(full_backup_path))] = full_backup_path
            else:
                link_dict[month_inc_file] = full_inc_file
                link_dict[os.path.join(month_dir, os.path.basename(full_backup_path))] = full_backup_path
                link_dict[daily_inc_file] = full_inc_file
                link_dict[os.path.join(daily_dir, os.path.basename(full_backup_path))] = full_backup_path
        else:
            if period == 'day':
                copy_dict[daily_inc_file] = full_inc_file
            else:
                copy_dict[month_inc_file] = full_inc_file
                copy_dict[daily_inc_file] = full_inc_file
    else:
        symlink_dir = ''

        if period == 'day':
            old_meta_info = specific_function.parser_json(full_inc_file)
            new_meta_info = get_index(target, exclude_list)

            general_inc_backup_dir = daily_dir

            general_dirs_for_log = general_function.get_dirs_for_log(general_inc_backup_dir, remote_dir, storage)
            general_function.create_dirs(job_name=job_name, dirs_pairs={general_inc_backup_dir:general_dirs_for_log})

            if not os.path.isfile(daily_inc_file):
                with open(daily_inc_file, "w") as index_file:
                    json.dump(new_meta_info, index_file)
        else:
            if int(date_day_of_month) == 1:
                # It is necessary to collect monthly incremental backup relative to the year copy
                old_meta_info = specific_function.parser_json(full_inc_file)
                new_meta_info = get_index(target, exclude_list)

                general_inc_backup_dir = month_dir

                # It is also necessary to make a symlink for inc files and backups to the directory with the first decade
                symlink_dir = daily_dir

                general_dirs_for_log = general_function.get_dirs_for_log(general_inc_backup_dir, remote_dir, storage)
                symlink_dirs_for_log = general_function.get_dirs_for_log(symlink_dir, remote_dir, storage)
                general_function.create_dirs(job_name=job_name, dirs_pairs={general_inc_backup_dir:general_dirs_for_log, symlink_dir:symlink_dirs_for_log})

                with open(month_inc_file, "w") as index_file:
                    json.dump(new_meta_info, index_file)

            elif int(date_day_of_month) == 11 or int(date_day_of_month) == 21:
                # It is necessary to collect a ten-day incremental backup relative to a monthly copy
                
                try:
                    old_meta_info = specific_function.parser_json(month_inc_file)
                except general_function.MyError as e:
                    log_and_mail.writelog('ERROR', "Couldn't open old month meta info file '%s': %s!" %(month_inc_file, e),
                                        config.filelog_fd, job_name)
                    return 2

                new_meta_info = get_index(target, exclude_list)

                general_inc_backup_dir = daily_dir
                general_dirs_for_log = general_function.get_dirs_for_log(general_inc_backup_dir, remote_dir, storage)
                general_function.create_dirs(job_name=job_name, dirs_pairs={general_inc_backup_dir:general_dirs_for_log}) 

                with open(daily_inc_file, "w") as index_file:
                    json.dump(new_meta_info, index_file)
            else:
                # It is necessary to collect a normal daily incremental backup relative to a ten-day copy
                
                try:
                    old_meta_info = specific_function.parser_json(daily_inc_file)
                except general_function.MyError as e:
                    log_and_mail.writelog('ERROR', "Couldn't open old decade meta info file '%s': %s!" %(daily_inc_file, e),
                                        config.filelog_fd, job_name)
                    return 2

                new_meta_info = get_index(target, exclude_list)

                general_inc_backup_dir = daily_dir
                general_dirs_for_log = general_function.get_dirs_for_log(general_inc_backup_dir, remote_dir, storage)
                general_function.create_dirs(job_name=job_name, dirs_pairs={general_inc_backup_dir:general_dirs_for_log})


        # Calculate the difference between the old and new file states
        diff_json = compute_diff(new_meta_info, old_meta_info)

        inc_backup_path = general_function.get_full_path(   general_inc_backup_dir,
                                                            backup_file_name, 
                                                            'tar',
                                                            gzip)

        # Define the list of files that need to be included in the archive
        target_change_list = diff_json['modify']

        # Form GNU.dumpdir headers
        dict_directory = {}  # Dict to store pairs like dir:GNU.dumpdir

        excludes = r'|'.join([fnmatch.translate(x)[:-7] for x in general_files_func.EXCLUDE_FILES]) or r'$.'

        for dir_name, dirs, files in os.walk(target):
            first_level_files = []

            if re.match(excludes, dir_name):
                continue

            for file in files:
                if re.match(excludes, os.path.join(dir_name, file)):
                    continue

                first_level_files.append(file)

            first_level_subdirs = dirs
            dict_directory[dir_name] = get_gnu_dumpdir_format(diff_json, dir_name, target, excludes, first_level_subdirs, first_level_files)

        create_inc_tar(inc_backup_path, remote_dir, dict_directory, target_change_list, gzip, job_name, storage, host, share)

        if symlink_dir:
            if storage in 'local, scp':
                link_dict[daily_inc_file] = month_inc_file
            else:
                copy_dict[daily_inc_file] = month_inc_file

    if link_dict:
        for key in link_dict.keys():
            src = link_dict[key]
            dst = key

            try:
                general_function.create_symlink(src, dst)
            except general_function.MyError as err:
                log_and_mail.writelog('ERROR', "Can't create symlink %s -> %s: %s" %(src, dst, err),
                                          config.filelog_fd, job_name)

    if copy_dict:
        for key in copy_dict.keys():
            src = copy_dict[key]
            dst = key

            try:
                general_function.copy_ofs(src, dst)
            except general_function.MyError as err:
                log_and_mail.writelog('ERROR', "Can't copy %s -> %s: %s" %(src, dst, err),
                                      config.filelog_fd, job_name)

def get_gnu_dumpdir_format(diff_json, dir_name, backup_dir, excludes, first_level_subdirs, first_level_files):
    ''' The function on the input receives a dictionary with modified files.

    '''

    delimiter = '\0'
    not_modify_special_symbol = 'N'
    modify_special_symbol = 'Y'
    directory_special_symbol = 'D'

    general_dict = {}

    if first_level_subdirs:
        for i in first_level_subdirs:
            general_dict[i] = directory_special_symbol

    if first_level_files:
        for file in first_level_files:
            if os.path.join(dir_name, file) in diff_json['modify']:
                general_dict[file] = modify_special_symbol
            else:
                general_dict[file] = not_modify_special_symbol

    keys = list(general_dict.keys())
    keys.sort()

    result = ''
    for i in range(len(keys)):
        result += general_dict.get(keys[i]) + keys[i] + delimiter

    result += delimiter

    return result


def get_index(backup_dir, exclude_list):
    """ Return a tuple containing:
    - a dict: filepath => ctime
    """

    file_index = {}

    excludes = r'|'.join([fnmatch.translate(x)[:-7] for x in general_files_func.EXCLUDE_FILES]) or r'$.'

    for root, dirs, filenames in os.walk(backup_dir):

        filenames = [os.path.join(root, f) for f in filenames]
        filenames = [f for f in filenames if not re.match(excludes, f)]

        for f in filenames:
            if os.path.isfile(f):
                file_index[f] = os.path.getmtime(f)

    return file_index


def compute_diff(new_meta_info, old_meta_info):
    data = {}

    created_files = list(set(new_meta_info.keys()) - set(old_meta_info.keys()))
    updated_files = []
    
    data['modify'] = []
    data['not_modify'] = []

    for f in set(old_meta_info.keys()).intersection(set(new_meta_info.keys())):
            try:
                if new_meta_info[f] != old_meta_info[f]:
                    updated_files.append(f)
                else:
                    data['not_modify'].append(f)
            except KeyError:
                # Occurs when in one of the states (old or new) one and the same path
                # are located both the broken and normal file
                updated_files.append(f)

    data['modify'] = created_files + updated_files

    return data


def create_inc_tar(path_to_tarfile, remote_dir, dict_directory, target_change_list, gzip, job_name, storage, host, share):
    ''' The function creates an incremental backup based on the GNU.dumpdir header in the PAX format.

    '''

    dirs_for_log = general_function.get_dirs_for_log(os.path.dirname(path_to_tarfile), remote_dir, storage)
    file_for_log = os.path.join(dirs_for_log, os.path.basename(path_to_tarfile))

    try:
        if gzip:
            out_tarfile = tarfile.open(path_to_tarfile, mode='w:gz', format=tarfile.PAX_FORMAT) 
        else:
            out_tarfile = tarfile.open(path_to_tarfile, mode='w:', format=tarfile.PAX_FORMAT)

        for i in dict_directory.keys():
            meta_file = out_tarfile.gettarinfo(name=i)
            pax_headers = {
                            'GNU.dumpdir': dict_directory.get(i)
                          }
            meta_file.pax_headers = pax_headers
            out_tarfile.addfile(meta_file)

        for i in target_change_list:
            if os.path.exists(i):
                out_tarfile.add(i)

        out_tarfile.close()
    except tarfile.TarError as err:
        if storage == 'local':
            log_and_mail.writelog('ERROR', "Can't create incremental '%s' archive on '%s' storage: %s" %(file_for_log, storage, err),
                                config.filelog_fd, job_name)
        elif storage == 'smb':
            log_and_mail.writelog('ERROR', "Can't create incremental '%s' archive in '%s' share on '%s' storage(%s): %s" %(file_for_log, share, storage, host, err),
                                config.filelog_fd, job_name)
        else:
            log_and_mail.writelog('ERROR', "Can't create incremental '%s' archive on '%s' storage(%s): %s" %(file_for_log, storage, host, err),
                                  config.filelog_fd, job_name)
        return False
    else:
        if storage == 'local':
            log_and_mail.writelog('INFO', "Successfully created incremental '%s' archive on '%s' storage." %(file_for_log, storage),
                                config.filelog_fd, job_name)
        elif storage == 'smb':
            log_and_mail.writelog('INFO', "Successfully created incremental '%s' archive in '%s' share on '%s' storage(%s)." %(file_for_log, share, storage, host),
                                config.filelog_fd, job_name)
        else:
            log_and_mail.writelog('INFO', "Successfully created incremental '%s' archive on '%s' storage(%s)." %(file_for_log, storage, host),
                                config.filelog_fd, job_name)
        return True


