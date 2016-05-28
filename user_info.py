import os
import pandas as pd


ROOT_FOLDER = 'F:\\mathematics_and_python'

# First .csv should be largest
USER_INFO_FILES = ['course_memberships.csv',
        'users_courses__certificate_payments.csv',
        'users.csv',
        'course_grades.csv',
        'feedback_course_ratings.csv']

LOG = True


def log_f(message):
    if LOG:
        print('> {0}'.format(message))


build_path = lambda folder: os.path.join(ROOT_FOLDER, folder)


def intersec_column_names(df1, df2):
    """get identical column names"""
    return list(set(df1.columns) & set(df2.columns))


# Df's are merged to the left, first dataframe should be largest
def merge_dfs(data_files):
    """merge all .csv files from data_files to single dataframe"""
    log_f('start merge_dfs')
    log_f('\t' + data_files[0])
    user_df = pd.read_csv(build_path(data_files[0]))
    for filename in data_files[1:]:
        log_f('\t' + filename)
        tmp_df = pd.read_csv(build_path(filename))
        intersect_cols = intersec_column_names(user_df, tmp_df)
        user_df = pd.merge(user_df, tmp_df, on=intersect_cols, how='left')
    log_f('finish merge_dfs')
    return user_df


def save_df(df, path):
    log_f('save_df')
    df.to_csv(path, index=False)
    log_f('finish save_df')


def merge_user_info(user_info_files, res_user_info_filename):
    """merge user_info_files to single files"""
    log_f('merge_user_info')
    save_df(merge_dfs(user_info_files), res_user_info_filename)
    log_f('finish merge_user_info')


if __name__ == '__main__':
    merge_user_info(USER_INFO_FILES, 'user_info.csv')
    log_f('done')
