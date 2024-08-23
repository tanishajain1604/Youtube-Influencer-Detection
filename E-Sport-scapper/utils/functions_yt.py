from collections import defaultdict
import pandas as pd
import pyrsm as rsm
import numpy as np


def get_channel_info(channel_ids, youtube):

    """" 
    Function to obtain channel information from Youtube channels. 
    Returns a pandas DF with channel info.
    """
    
    # 1. Removing duplicates from the list:
    
    channel_ids = list(set(channel_ids))
    
    # 2. In case the list is over 50 channels:
    # Create a list that only includes up to 50 videos per entry
    max50list = []

    for i in range(len(channel_ids)//50+1):
        max50list.append(','.join(channel_ids[i*50:min(i*50+50, len(channel_ids))]))
    
    # 3. Now we obtain the channel info:
    
    channels_info = []

    for ch_list in max50list:
        request = youtube.channels().list(
            part = "id, snippet, statistics, topicDetails",
            id = ch_list,
            fields = "items(id, snippet(title, description, publishedAt), statistics, topicDetails)",
            maxResults=50,
            )
            # Query execution
        response = request.execute()
        channels_info += response['items']
    
    # 4. Now we put the relevant channel info into a DF:
    
    channels = defaultdict(list)

    for item in channels_info:
        # id
        ch_id = item['id']
        # title
        ch_title = item['snippet']['title'] if 'title' in item['snippet'] else None
        # description    
        ch_description = item['snippet']['description'] if 'description' in item['snippet'] else None
        # creation date
        ch_created = item['snippet']['publishedAt'] if 'publishedAt' in item['snippet'] else None
        # country
        ch_country = item['snippet']['country'] if 'country' in item['snippet'] else None
        # view count    
        ch_viewCount = item['statistics']['viewCount'] if 'viewCount' in item['statistics'] else None
        # subscriber count    
        ch_subscriberCount = item['statistics']['subscriberCount'] if 'subscriberCount' in item['statistics'] else None
        # video count
        ch_videoCount = item['statistics']['videoCount'] if 'videoCount' in item['statistics'] else None

        channels['ch_id'].append(ch_id)
        channels['ch_title'].append(ch_title)
        channels['ch_description'].append(ch_description)
        channels['ch_created'].append(ch_created)
        channels['ch_country'].append(ch_country)
        channels['ch_viewCount'].append(ch_viewCount)
        channels['ch_subscriberCount'].append(ch_subscriberCount)
        channels['ch_videoCount'].append(ch_videoCount)

    df_channels = pd.DataFrame.from_dict(channels)

    df_channels['ch_viewCount'] = df_channels['ch_viewCount'].astype(int)
    df_channels['ch_subscriberCount'] = df_channels['ch_subscriberCount'].astype(int)
    df_channels['ch_videoCount'] = df_channels['ch_videoCount'].astype(int)
    
    return(df_channels)



def get_videos_info(channel_list, youtube, max_results = 300, published_after = '2023-03-01T00:00:00Z'):

    """" 
    Function to obtain videos information from Youtube channels. 
    Returns a pandas DF with videos info.
    """
    
    # 1. Removing duplicates from the list:
    channel_list = list(set(channel_list))
    
    # 2. Obtaining the videos ids:
    video_ids = []
    
    for channel in channel_list:

        next_page_token = None

        while True:

            request = youtube.search().list(
                part = "id",
                type='video',
                channelId = channel,
                publishedAfter = published_after,
                fields = "items(id(videoId))",
                maxResults=min(50, max_results),
                pageToken=next_page_token
            )
            # Query execution
            response = request.execute()
            videos = response['items']
            for video in videos:
                video_ids.append(video['id']['videoId'])

            # Check if there are more pages to fetch
            next_page_token = response.get('nextPageToken')
            if not next_page_token or max_results <= 0:
                break

            # Adjust the remaining number of videos to retrieve
            max_results -= min(50, max_results)
    
    # 3. Removing duplicates from the list obtained:
    
    video_ids = list(set(video_ids))
    print(f"Total number of channels: {len(channel_list)}")
    print(f"Total number of videos: {len(video_ids)}")
    
    # 4. Create a list that only includes up to 50 videos per entry
    
    max50list = []

    for i in range(len(video_ids)//50+1):
        max50list.append(','.join(video_ids[i*50:min(i*50+50, len(video_ids))]))
    
    # 5. Now we're going to fetch the information from those videos
    
    vid_info = []

    for vid_list in max50list:
        request = youtube.videos().list(
            part = "id, snippet, statistics, topicDetails",
            id = vid_list,
            fields = "items(id, snippet(publishedAt, channelId, title, description), statistics)",
            maxResults=50
            )
            # Query execution
        response = request.execute()
        vid_info += response['items']
    
    # 6. Creating the DataFrame:
    
    videos = defaultdict(list)

    for item in vid_info:       
        # id
        vid_id = item['id']
        # channel id
        ch_id = item['snippet']['channelId'] if 'channelId' in item['snippet'] else None
        # title
        vid_title = item['snippet']['title'] if 'title' in item['snippet'] else None
        # description    
        vid_description = item['snippet']['description'] if 'description' in item['snippet'] else None
        # creation date
        vid_published = item['snippet']['publishedAt'] if 'publishedAt' in item['snippet'] else None
        # view count    
        vid_viewCount = item['statistics']['viewCount'] if 'viewCount' in item['statistics'] else None
        # subscriber count    
        vid_likeCount = item['statistics']['likeCount'] if 'likeCount' in item['statistics'] else None
        # video count
        vid_commentCount = item['statistics']['commentCount'] if 'commentCount' in item['statistics'] else None
            
        videos['vid_id'].append(vid_id)
        videos['ch_id'].append(ch_id)
        videos['vid_title'].append(vid_title)
        videos['vid_description'].append(vid_description)
        videos['vid_published'].append(vid_published)
        videos['vid_viewCount'].append(vid_viewCount)
        videos['vid_likeCount'].append(vid_likeCount)
        videos['vid_commentCount'].append(vid_commentCount)
        
    df_videos = pd.DataFrame.from_dict(videos)
        
    # Some cleaning:
    df_videos['vid_viewCount'].fillna(0, inplace=True)
    df_videos['vid_likeCount'].fillna(0, inplace=True)
    df_videos['vid_commentCount'].fillna(0, inplace=True)
    # Adjust data types:
    df_videos['vid_viewCount'] = df_videos['vid_viewCount'].astype(int)
    df_videos['vid_likeCount'] = df_videos['vid_likeCount'].astype(int)
    df_videos['vid_commentCount'] = df_videos['vid_commentCount'].astype(int)
        
    return(df_videos)



def get_commenters_info(video_ids, youtube):
    
    """" 
    Function to obtain commenters information from Youtube videos. 
    Returns a pandas DF with commenters info.
    """

    comments = defaultdict(list)
    
    for vid in video_ids:

        max_results = 3000
        next_page_token = None

        while True:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=vid,
                maxResults=min(100, max_results),
                pageToken=next_page_token
            )
            # Obtains the comments for a particular video. If comments are disabled (403 error), continue
            try:
                response = request.execute()
            except:
                break

            for item in response['items']:
                try:
                    
                    comment = item['snippet']['topLevelComment']
                    commenter_id = comment['snippet']['authorChannelId']['value']
                    comment_id = comment['id']
                    comments[vid].append((commenter_id, comment_id))
                except:
                    continue

            # Check if there are more pages to fetch
            next_page_token = response.get('nextPageToken')
            if not next_page_token or max_results <= 0:
                break

            # Adjust the remaining number of videos to retrieve
            max_results -= min(100, max_results) 
    
    
    # Convert to DF
    comments_df = pd.DataFrame.from_dict(comments, orient = 'index').transpose()
    # Convert to long format
    comments_long = pd.melt(comments_df, var_name='vid_id', value_name='com_id')
    # Drop null rows
    comments_long = comments_long.dropna()
    # Unzip
    comments_long[['commenter_id', 'comment_id']] = comments_long['com_id'].apply(lambda x: pd.Series(x))
    comments_long = comments_long.drop('com_id', axis = 1)
    
    return(comments_long)
