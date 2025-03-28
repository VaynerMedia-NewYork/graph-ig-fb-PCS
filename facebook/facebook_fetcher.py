import requests
import json
import time
import logging
import re
import pandas as pd
import traceback
from datetime import datetime
from fuzzywuzzy import fuzz
from typing import Dict, List, Optional, Tuple, Any
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FacebookCommentsFetcher:
    """
    Fetches Facebook comments with robust error handling, pagination support, and rate limit management.
    """
    
    def __init__(self, access_token=None):
        """Initialize with access token."""
        # Initialize time tracking
        self.start_time = datetime.now()
        logger.info(f"Initializing FacebookCommentsFetcher at {self.start_time.isoformat()}")
        
        # Track API calls for debugging rate limits
        self.api_call_count = 0
        
        try:
            # Get credentials
            if access_token:
                self.user_access_token = access_token
            else:
                # Read from config file or environment variable
                self.user_access_token = os.getenv('access_token')
                if not self.user_access_token:
                    raise ValueError("No Facebook access token provided. Set FACEBOOK_ACCESS_TOKEN environment variable.")
            
            # Initialize page token cache
            self.page_tokens = {}
            
            # Initialize page dictionary with page names to IDs and tokens
            logger.info("Initializing page dictionary...")
            self.page_dict = self.get_facebook_page_id_and_token(self.user_access_token)
            
            # Initialize collector for comments
            self.all_comments = []
            self.processed_count = 0
            self.failed_links = []
            
            # Output file info
            self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.output_path = f"facebook_comments_{self.timestamp}.csv"
            
        except Exception as e:
            logger.error(f"Error during initialization: {str(e)}")
            traceback.print_exc()
            raise
    
    def get_facebook_page_id_and_token(self, access_token):
        """
        Get all Facebook Pages and their access tokens that the user has access to.
        
        Args:
            access_token (str): The user access token for the Graph API.
        
        Returns:
            dict: Dictionary of page names to {id, access_token} objects
        """
        logger.info("Retrieving Facebook pages and access tokens")
        
        base_url = "https://graph.facebook.com/v22.0/me/accounts"
        params = {
            'access_token': access_token,
            'limit': 100
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=30)
            self.api_call_count += 1
            
            if response.status_code != 200:
                logger.error(f"Error getting pages: {response.text}")
                return {}
            
            pages = response.json().get('data', [])
            if not pages:
                logger.warning("No pages found or no access to pages")
                return {}
            
            # Create a dictionary of page names to page details
            page_dict = {}
            for page in pages:
                page_name = page.get('name', '')
                page_id = page.get('id', '')
                page_token = page.get('access_token', '')
                
                logger.info(f"Found page: {page_name} (ID: {page_id})")
                page_dict[page_name] = {
                    'id': page_id,
                    'access_token': page_token
                }
            
            return page_dict
            
        except Exception as e:
            logger.error(f"Error getting Facebook pages and tokens: {str(e)}")
            return {}
    

    def get_page_details_by_name(self, page_name):
        """
        Get page ID and access token by matching page name.

        Args:
            page_name (str): The name of the Facebook page or a comma-separated list of names.

        Returns:
            tuple: (page_id, page_token) or (None, None) if not found
        """
        # Split the page names if there are multiple
        page_names = [name.strip() for name in page_name.split(',')] if page_name else []

        best_match = None
        best_score = 0

        for name in page_names:
            # Try exact match first
            for page_name, details in self.page_dict.items():
                if page_name.lower() == name.lower():
                    logger.info(f"Found exact match for '{name}': {page_name}")
                    return details['id'], details['access_token']

            # Try fuzzy match if exact match fails
            for page_name, details in self.page_dict.items():
                score = fuzz.ratio(name.lower(), page_name.lower())
                if score > best_score:
                    best_score = score
                    best_match = (page_name, details)

        # If a good fuzzy match is found
        if best_match and best_score > 30:  # 30% similarity threshold
            logger.info(f"Using best fuzzy match for '{page_name}': {best_match[0]} (score: {best_score}%)")
            return best_match[1]['id'], best_match[1]['access_token']

        logger.warning(f"Could not find a matching page for '{page_name}'")
        return None, None

    
    def extract_post_id_from_url(self, url):
        """
        Extract the post ID from a Facebook post URL.
        
        Args:
            url (str): The URL of the Facebook post.
        
        Returns:
            tuple: (page_id, post_id) or (None, None) if not found.
        """
        logger.info(f"Extracting post ID from URL: {url}")
        
        # Format: facebook.com/PageID_PostID
        underscore_match = re.search(r'facebook\.com/(\d+)_(\d+)', url)
        if underscore_match:
            page_id = underscore_match.group(1)
            post_id = underscore_match.group(2)
            logger.info(f"Extracted page ID {page_id} and post ID {post_id} from underscore format")
            return page_id, post_id
        
        # Format: facebook.com/reel/ReelID
        reel_match = re.search(r'facebook\.com/reel/(\d+)', url)
        if reel_match:
            reel_id = reel_match.group(1)
            logger.info(f"Extracted reel ID from reel URL: {reel_id}")
            return None, reel_id  # Return None for page_id since it's a reel
        
        # Format: facebook.com/permalink.php?story_fbid=PostID&id=PageID
        permalink_match = re.search(r'facebook\.com/permalink\.php\?.*?story_fbid=(\d+).*?id=(\d+)', url)
        if permalink_match:
            post_id = permalink_match.group(1)
            page_id = permalink_match.group(2)
            logger.info(f"Extracted post ID {post_id} and page ID {page_id} from permalink URL")
            return page_id, post_id
        
        # Format: facebook.com/video.php?v=VideoID
        video_match = re.search(r'facebook\.com/video\.php\?.*?v=(\d+)', url)
        if video_match:
            video_id = video_match.group(1)
            logger.info(f"Extracted video ID from video URL: {video_id}")
            return None, video_id  # Return None for page_id since it's a video

        logger.warning("Could not extract post ID from URL using any known pattern")
        return None, None
    
    def get_page_feed(self, page_id, page_token, limit=100):
        """
        Get the feed (posts) for a Facebook page.
        
        Args:
            page_id (str): The ID of the Facebook page.
            page_token (str): The page access token.
            limit (int): Maximum number of posts to retrieve.
        
        Returns:
            list: A list of posts from the page's feed.
        """
        logger.info(f"Getting feed for page ID: {page_id}")
        
        base_url = f"https://graph.facebook.com/v19.0/{page_id}/feed"
        params = {
            'access_token': page_token,
            'limit': limit,
            'fields': 'id,message,created_time,permalink_url'
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=30)
            self.api_call_count += 1
            
            if response.status_code == 200:
                feed_data = response.json()
                posts = feed_data.get('data', [])
                logger.info(f"Retrieved {len(posts)} posts from page feed")
                return posts
            else:
                logger.error(f"Error fetching page feed: {response.text}")
                return []
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception when getting page feed: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error getting page feed: {str(e)}")
            return []
    
    def find_post_by_url_or_content(self, page_id, page_token, post_url, post_content):
        """
        Find a Facebook post by URL or content within a page's feed.
        
        Args:
            page_id (str): The ID of the Facebook page.
            page_token (str): The page access token.
            post_url (str): The URL of the post to find (optional).
            post_content (str): Some content from the post to match (optional).
        
        Returns:
            str: The ID of the found post, or None if not found.
        """
        logger.info(f"Searching for post with URL: {post_url}")
        
        # If we have a URL, try to extract the post ID directly
        if post_url:
            page_id_from_url, post_id_from_url = self.extract_post_id_from_url(post_url)
            
            # If we got a valid post ID (and possibly page ID)
            if post_id_from_url:
                # Construct the full post ID (may include page ID)
                full_post_id = None
                
                # If we have both page ID from URL and post ID
                if page_id_from_url and post_id_from_url:
                    full_post_id = f"{page_id_from_url}_{post_id_from_url}"
                # If we have page ID from function parameter and post ID from URL
                elif page_id and post_id_from_url:
                    full_post_id = f"{page_id}_{post_id_from_url}"
                # If we only have post ID (try it directly)
                elif post_id_from_url:
                    full_post_id = post_id_from_url
                
                if full_post_id:
                    logger.info(f"Attempting to validate post ID: {full_post_id}")
                    # Validate the post ID
                    try:
                        test_url = f"https://graph.facebook.com/v22.0/{full_post_id}"
                        params = {'access_token': page_token, 'fields': 'id'}
                        response = requests.get(test_url, params=params, timeout=30)
                        self.api_call_count += 1
                        
                        if response.status_code == 200:
                            logger.info(f"Validated post ID: {full_post_id}")
                            return full_post_id
                        else:
                            logger.warning(f"Post ID {full_post_id} is not valid: {response.text}")
                            
                            # If we have a page ID, try the alternative format
                            if page_id and not page_id_from_url:
                                alt_full_post_id = f"{page_id}_{post_id_from_url}"
                                logger.info(f"Trying alternative post ID format: {alt_full_post_id}")
                                
                                alt_test_url = f"https://graph.facebook.com/v22.0/{alt_full_post_id}"
                                alt_response = requests.get(alt_test_url, params=params, timeout=30)
                                self.api_call_count += 1
                                
                                if alt_response.status_code == 200:
                                    logger.info(f"Validated alternative post ID: {alt_full_post_id}")
                                    return alt_full_post_id
                    except Exception as e:
                        logger.error(f"Error validating post ID: {str(e)}")
        
        # If we couldn't get a valid ID from the URL, search the page's feed
        posts = self.get_page_feed(page_id, page_token, limit=100)
        
        # Try to match by URL first
        if post_url:
            for post in posts:
                if 'permalink_url' in post and post_url in post['permalink_url']:
                    logger.info(f"Found post by URL match: {post['id']}")
                    return post['id']
        
        # If URL matching failed and we have content, try content matching
        if post_content and len(post_content) > 10:  # Ensure there's enough content to match
            best_match = None
            best_score = 0
            
            for post in posts:
                if 'message' in post and post['message']:
                    score = fuzz.partial_ratio(post_content.lower(), post['message'].lower())
                    if score > best_score:
                        best_score = score
                        best_match = post
            
            if best_match and best_score > 80:  # 80% similarity threshold
                logger.info(f"Found post by content match (score: {best_score}%): {best_match['id']}")
                return best_match['id']
        
        logger.warning("Could not find the post in the page's feed")
        return None
    
    def get_facebook_comments(self, post_id, page_token, limit=20000):
        """
        Fetch comments from a Facebook post with pagination support.
        
        Args:
            post_id (str): The ID of the Facebook post.
            page_token (str): The page access token.
            limit (int): Maximum number of comments to retrieve (None for all).
        
        Returns:
            list: A list of comments from the post.
        """
        logger.info(f"Fetching comments for post: {post_id}")
        
        base_url = f"https://graph.facebook.com/v19.0/{post_id}/comments"
        params = {
            'access_token': page_token,
            'summary': 'true',  # Get summary information
            'filter': 'stream',  # Get all comments
            'limit': 100,  # Maximum per page
            'fields': 'id,message,created_time,like_count,from,attachment,comment_count,parent'  # Get detailed information
        }
        
        all_comments = []
        next_page = base_url
        page_count = 0
        
        try:
            # Loop through all pages of comments
            while next_page and (limit is None or len(all_comments) < limit):
                page_count += 1
                logger.info(f"Fetching comments page {page_count}...")
                
                response = requests.get(next_page, params=params, timeout=30)
                self.api_call_count += 1
                
                if response.status_code != 200:
                    logger.error(f"Error fetching comments page {page_count}: {response.text}")
                    break
                
                data = response.json()
                comments_page = data.get('data', [])
                
                if not comments_page:
                    logger.info(f"No more comments found on page {page_count}")
                    break
                
                # Add these comments
                all_comments.extend(comments_page)
                logger.info(f"Retrieved {len(comments_page)} comments from page {page_count} (total: {len(all_comments)})")
                
                # Check if we've reached the specified limit
                if limit is not None and len(all_comments) >= limit:
                    logger.info(f"Reached specified limit of {limit} comments")
                    break
                
                # Check for more pages of comments
                if 'paging' in data and 'next' in data['paging']:
                    next_page = data['paging']['next']
                    # We don't need params anymore since the URL contains them
                    params = {}
                    logger.info("Found next page of comments")
                else:
                    logger.info("No more pages of comments available")
                    next_page = None
                
                # Add a delay to avoid rate limiting
                if next_page:
                    time.sleep(1)
            
            # Get summary information if available
            summary = data.get('summary', {}) if 'summary' in data else {}
            total_count = summary.get('total_count', 0)
            if total_count > 0:
                logger.info(f"Total comments according to summary: {total_count}")
            
            logger.info(f"Actually retrieved: {len(all_comments)} comments")
            
            # Fetch replies for each comment
            comments_with_replies = []
            for comment in all_comments:
                # Check if this comment has replies
                if comment.get('comment_count', 0) > 0:
                    # Fetch replies for this comment
                    replies = self.get_comment_replies(comment['id'], page_token)
                    comment['replies'] = replies
                    logger.info(f"Retrieved {len(replies)} replies for comment {comment['id']}")
                else:
                    comment['replies'] = []
                
                comments_with_replies.append(comment)
            
            return comments_with_replies
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception when fetching comments: {str(e)}")
            return all_comments if all_comments else []
        except Exception as e:
            logger.error(f"Error fetching comments: {str(e)}")
            return all_comments if all_comments else []
    
    def get_comment_replies(self, comment_id, page_token):
        """
        Fetch replies to a specific comment.
        
        Args:
            comment_id (str): The ID of the comment.
            page_token (str): The page access token.
        
        Returns:
            list: A list of replies to the comment.
        """
        logger.info(f"Fetching replies for comment: {comment_id}")
        
        base_url = f"https://graph.facebook.com/v19.0/{comment_id}/comments"
        params = {
            'access_token': page_token,
            'limit': 100,  # Maximum per page
            'fields': 'id,message,created_time,like_count,from,attachment'
        }
        
        all_replies = []
        next_page = base_url
        
        try:
            # Loop through all pages of replies
            while next_page:
                response = requests.get(next_page, params=params, timeout=30)
                self.api_call_count += 1
                
                if response.status_code != 200:
                    logger.error(f"Error fetching replies: {response.text}")
                    break
                
                data = response.json()
                replies_page = data.get('data', [])
                
                if not replies_page:
                    break
                
                # Add these replies
                all_replies.extend(replies_page)
                
                # Check for more pages of replies
                if 'paging' in data and 'next' in data['paging']:
                    next_page = data['paging']['next']
                    # We don't need params anymore since the URL contains them
                    params = {}
                else:
                    next_page = None
                
                # Add a delay to avoid rate limiting
                if next_page:
                    time.sleep(1)
            
            return all_replies
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception when fetching replies: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error fetching replies: {str(e)}")
            return []
    
    def format_comments_for_output(self, comments, client, url):
        """
        Format comments for output in a structured way.
        
        Args:
            comments (list): The list of comments to format.
            client (str): The client name.
            url (str): The post URL.
        
        Returns:
            list: Formatted comments with replies.
        """
        formatted_comments = []
        
        for i, comment in enumerate(comments, 1):
            # Format main comment
            comment_date = comment.get('created_time', '')
            from_name = comment.get('from', {}).get('name', 'Unknown')
            
            main_comment = {
                'id': i,
                'sub_id': '',
                'date': comment_date,
                'week': '',  # Will be calculated later when we have the DataFrame
                'likes': comment.get('like_count', 0),
                'live_video_timestamp': '-',
                'comment': comment.get('message', ''),
                'image_urls': '',
                'view_source': 'view comment',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'client': client,
                'url': url,
                'author': from_name,
                'platform': 'facebook'
            }
            formatted_comments.append(main_comment)
            
            # Format replies if they exist
            if 'replies' in comment and comment['replies']:
                for j, reply in enumerate(comment['replies'], 1):
                    sub_id = f"{i}.{j}"
                    reply_date = reply.get('created_time', '')
                    reply_from_name = reply.get('from', {}).get('name', 'Unknown')
                    
                    reply_comment = {
                        'id': i,
                        'sub_id': sub_id,
                        'date': reply_date,
                        'week': '',  # Will be calculated later when we have the DataFrame
                        'likes': reply.get('like_count', 0),
                        'live_video_timestamp': '-',
                        'comment': reply.get('message', ''),
                        'image_urls': '',
                        'view_source': 'view comment',
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'client': client,
                        'url': url,
                        'author': reply_from_name,
                        'platform': 'facebook'
                    }
                    formatted_comments.append(reply_comment)
        
        return formatted_comments
    

    def process_link(self, post_url, access_token, client=None):
        """Process a single Facebook post URL and fetch its comments."""
        if not post_url or 'facebook' not in post_url:
            logger.warning(f"URL does not contain 'facebook' or is empty: {post_url}")
            return []

        logger.info(f"Processing post: {post_url}")
        logger.info(f"Client: {client}")

        try:
            # Handle multiple mapped names
            if client:
                # Make sure client is a string before splitting
                if isinstance(client, str):
                    mapped_names = [name.strip() for name in client.split(',')]
                else:
                    # If client is not a string (e.g., None, nan, or a number)
                    logger.warning(f"Client value is not a string: {client}, type: {type(client)}")
                    mapped_names = []

                # Try each mapped name
                for name in mapped_names:
                    if not name:  # Skip empty names
                        continue

                    # Get Page ID and token for this client
                    page_id, page_token = self.get_page_details_by_name(name)
                    if not page_id or not page_token:
                        logger.error(f"Could not find Facebook page for client: {name}. Check the page name.")
                        continue

                    # Extract post ID from URL
                    url_page_id, url_post_id = self.extract_post_id_from_url(post_url)

                    # Construct the full post ID
                    full_post_id = None

                    # If we got page_id and post_id from URL
                    if url_page_id and url_post_id:
                        full_post_id = f"{url_page_id}_{url_post_id}"
                    # If we only got post_id from URL but have page_id from client match
                    elif page_id and url_post_id:
                        full_post_id = f"{page_id}_{url_post_id}"

                    # If we couldn't construct post ID directly, try searching
                    if not full_post_id:
                        full_post_id = self.find_post_by_url_or_content(page_id, page_token, post_url, None)

                    if not full_post_id:
                        logger.error(f"Could not find post ID for URL: {post_url}")
                        continue

                    logger.info(f"Using full post ID: {full_post_id}")

                    # Get comments for the post (with limit of 20,000 as requested)
                    comments = self.get_facebook_comments(full_post_id, page_token, limit=20000)

                    if not comments:
                        logger.info(f"No comments found for this post.")
                        continue

                    # Format comments for output
                    formatted_comments = self.format_comments_for_output(comments, name, post_url)

                    logger.info(f"Processed {len(formatted_comments)} comments for post: {post_url}")

                    # Add comments to our collection
                    self.all_comments.extend(formatted_comments)

                    # Save the comments to a file
                    self.save_comments()

                    return formatted_comments

                # If we reached here, none of the mapped names worked
                logger.error(f"Could not process post with any of the provided client names")
                self.failed_links.append(post_url)
                return []
            else:
                # No client provided
                logger.error(f"No client name provided for post: {post_url}")
                self.failed_links.append(post_url)
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception processing post: {str(e)}")
            if "429" in str(e):
                logger.warning("Rate limit detected. Pausing processing.")
                time.sleep(60)  # Sleep for a minute on rate limit
            self.failed_links.append(post_url)
            return []
        except Exception as e:
            logger.error(f"Error processing post: {str(e)}")
            traceback.print_exc()
            self.failed_links.append(post_url)
            return []
    
    def save_comments(self):
        """Save collected comments to CSV file."""
        if not self.all_comments:
            logger.info("No comments to save")
            return None
        
        try:
            # Create DataFrame from comments
            logger.info("Creating DataFrame from comments...")
            comments_df = pd.DataFrame(self.all_comments)
            
            # Process the data
            logger.info("Processing dates and adding week column...")
            comments_df['date'] = pd.to_datetime(comments_df['date'])
            comments_df['week'] = comments_df['date'] - pd.to_timedelta(comments_df['date'].dt.weekday, unit='D')
            comments_df['week'] = comments_df['week'].dt.strftime('%Y-%m-%d')
            
            # Reorder columns to match Instagram format
            ordered_columns = [
                'id', 'sub_id', 'date', 'week', 'likes', 'live_video_timestamp',
                'comment', 'image_urls', 'view_source', 'timestamp',
                'client', 'url', 'platform', 'author'
            ]
            
            # Filter columns to only those we have
            ordered_columns = [col for col in ordered_columns if col in comments_df.columns]
            comments_df = comments_df[ordered_columns]
            
            # Save to CSV
            logger.info(f"Saving {len(comments_df)} comments to {self.output_path}")
            comments_df.to_csv(self.output_path, index=False)
            logger.info(f"Comments saved to {self.output_path}")
            
            return self.output_path
            
        except Exception as e:
            logger.error(f"Error saving comments: {str(e)}")
            traceback.print_exc()
            
            # Try to save in a different format as backup
            try:
                backup_path = f"facebook_comments_backup_{self.timestamp}.json"
                with open(backup_path, 'w') as f:
                    json.dump(self.all_comments, f)
                logger.info(f"Backup comments saved to {backup_path}")
                return backup_path
            except:
                logger.error("Failed to save backup")
                return None
            
