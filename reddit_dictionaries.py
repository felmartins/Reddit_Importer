from collections import OrderedDict
import praw

cmt_buffer = list()
user_buffer = {"author_fullname": [], \
   }

data_dict = OrderedDict(
                  {"submissions": 
                   {"id":                      [], \
                    "author_fullname":         [], \
                    "created_utc":             [], \
                    "subreddit_name_prefixed": [], \
                    "subreddit_id":            [], \
                    "title":                   [], \
                    "selftext":                [], \
                    "domain":                  [], \
                    "score":                   [], \
                    "upvote_ratio":            [], \
                    "num_comments":            [], \
                    "num_crossposts":          [], \
                    "total_awards_received":   [], \
                    "is_video":                [], \
                    "media_only":              [], \
                    "pinned":                  [], \
                    "stickied":                [], \
                    "removed_by_category":     [], \
                  }, \

                  "comments":
                  {"id":                     [], \
                  "author_fullname":         [], \
                  "body":                    [], \
                  "created_utc":             [], \
                  "subreddit_name_prefixed": [], \
                  "subreddit_id":            [], \
                  "link_id":                 [], \
                  "parent_id":               [], \
                  "score":                   [], \
                  "total_awards_received":   [], \
                  "gilded":                  [], \
                  "distinguished":           [], \
                  "is_submitter":            [], \
                  "stickied":                [], \
                  "removal_reason":          [], \
                }, \

                  "users":
                  {"fullname":                [], \
                   "name":                    [], \
                   "created_utc":             [], \
                   "link_karma":              [], \
                   "comment_karma":           [], \
                }, \

                  "subreddits":
                  {"name":                    [], \
                   "display_name_prefixed":   [], \
                   "title":                   [], \
                   "subscribers":             [], \
                   "quarantine":              [], \
                   "public_description":      [], \
                   "hide_ads":                [], \
                   "community_reviewed":      [], \
                   "over18":                  [], \
                   "created_utc":             [], \
                }, \

                  "network":
                  {"id":                         [], \
                   "created_utc":                [], \
                   "author_fullname":            [], \
                   "parent_id":                  [], \
                   "subreddit_id":               [], \
                   "interaction_type":           [], \
                }})

def empty_batch_storage(d_dict):
   for dictionary in [d_dict['submissions'], d_dict['comments'], d_dict['users'], d_dict['subreddits'], d_dict['network']]:
      for i in dictionary:
         dictionary[i] = []

def create_network(sub_data, cmt_data, data_dict):
   for key in data_dict['network']:
      reps = len(sub_data.index)
      if key in sub_data.columns.values.tolist():
         data_dict['network'][key].extend(sub_data[key].values.tolist())
      elif key == "interaction_type":
         for i in range(reps):
            data_dict['network'][key].append("Submission")
      elif key not in sub_data.columns.values.tolist():
         for i in range(reps):
            data_dict['network'][key].append("N/A")

   for key in data_dict['network']:
      reps = len(cmt_data.index)
      if key in cmt_data.columns.values.tolist():
         data_dict['network'][key].extend(cmt_data[key].values.tolist())
      elif key == "interaction_type":
         for i in range(reps):
            data_dict['network'][key].append("Comment / Reply")
      elif key not in cmt_data.columns.values.tolist():
         for i in range(reps):
            data_dict['network'][key].append("N/A")