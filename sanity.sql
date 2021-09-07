DELETE FROM rd_users WHERE rowid NOT IN (SELECT min(rowid) FROM rd_users GROUP BY fullname);
DELETE FROM rd_subreddits WHERE rowid NOT IN (SELECT min(rowid) FROM rd_subreddits GROUP BY name);
