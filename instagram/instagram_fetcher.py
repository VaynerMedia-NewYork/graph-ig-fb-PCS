
import requests
import json
import time
import pandas as pd
import logging
from datetime import datetime
import re
from fuzzywuzzy import fuzz

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InstagramFetcher:
    def __init__(self):
        self.base_url = "https://graph.facebook.com"
        self.api_version = "v22.0"
        self.all_comments = []
        self.failed_links = []
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_path = f"instagram_comments_{self.timestamp}.csv"
        

    def get_instagram_business_id(self, access_token, page_name=None):
        """
        Get the Instagram Business Account ID from a Facebook Page.

        Args:
            access_token (str): The access token for the Graph API.
            page_name (str): The name of the Facebook page (optional).

        Returns:
            str: The Instagram Business Account ID or None if not found.
        """
        # Step 1: Find the Page ID by searching for the page by name if provided
        if page_name:
            search_url = f"{self.base_url}/{self.api_version}/me/accounts?access_token={access_token}"
            logger.info(f"Searching for Facebook Pages matching '{page_name}'...")
            response = requests.get(search_url)
            if response.status_code != 200:
                logger.error(f"Error searching for pages: {response.text}")
                return None

            pages = response.json().get('data', [])
            if not pages:
                logger.warning("No pages found or no access to pages")
                return None

            # Try to find the exact page first
            page_id = None
            best_match = None
            best_score = 0

            for page in pages:
                page_name_from_api = page.get('name', '')
                logger.info(f"Found page: {page_name_from_api} (ID: {page.get('id')})")

                # Check for exact match
                if page_name_from_api.lower() == page_name.lower():
                    page_id = page.get('id')
                    logger.info(f"Found exact match for '{page_name}': {page_name_from_api}")
                    break

                # Calculate fuzzy match score
                score = fuzz.ratio(page_name.lower(), page_name_from_api.lower())
                if score > best_score:
                    best_score = score
                    best_match = page

            # If no exact match found, use the best fuzzy match if good enough
            if not page_id and best_score > 30:  # 30% similarity threshold
                page_id = best_match.get('id')
                logger.info(f"Using best fuzzy match for '{page_name}': {best_match.get('name')} (score: {best_score}%)")

            if not page_id:
                logger.warning(f"Could not find a matching page for '{page_name}'")
                return None
        else:
            logger.warning("No page name provided for Instagram Business ID lookup.")
            return None  # Return None if no page name is provided

        # Step 2: Get the Instagram Business Account ID connected to this Page
        ig_url = f"{self.base_url}/{self.api_version}/{page_id}?fields=instagram_business_account&access_token={access_token}"
        logger.info(f"Requesting Instagram Business Account...")
        response = requests.get(ig_url)
        if response.status_code != 200:
            logger.error(f"Error getting Instagram business account: {response.text}")
            return None

        instagram_data = response.json().get('instagram_business_account', {})
        if not instagram_data:
            logger.warning(f"No Instagram Business Account connected to this Page")
            return None

        ig_business_id = instagram_data.get('id')
        logger.info(f"Found Instagram Business Account ID: {ig_business_id}")
        return ig_business_id
    
    def search_instagram_media_with_extensive_pagination(self, ig_business_id, media_code, access_token):
        """
        Search for an Instagram media by its code with extensive pagination.
        Now includes collaboration partner detection.
        """
        # Get the first page of media with additional collaboration fields
        media_url = f"{self.base_url}/{self.api_version}/{ig_business_id}/media?fields=id,permalink,timestamp,collaborators,tagged_accounts,mentioned_profiles,branded_content_partner&limit=100&access_token={access_token}"
        
        media_id = None
        media_data = None
        next_page = media_url
        page_count = 0
        max_pages = 30  # Set a reasonable limit to avoid infinite loops
        
        # Keep track of earliest and latest media to understand the time range
        earliest_date = None
        latest_date = None
        
        logger.info(f"Searching for media code: {media_code}")
        
        # Loop through pages until we find the media or run out of pages
        while next_page and not media_id and page_count < max_pages:
            page_count += 1
            logger.info(f"Checking page {page_count} of media...")
            
            response = requests.get(next_page)
            if response.status_code != 200:
                logger.error(f"Error getting media list: {response.text}")
                return None, None
            
            data = response.json()
            media_list = data.get('data', [])
            
            # Update time range information
            if media_list:
                if not latest_date and 'timestamp' in media_list[0]:
                    latest_date = media_list[0]['timestamp']
                    logger.info(f"Latest media date: {latest_date}")
                
                if 'timestamp' in media_list[-1]:
                    earliest_date = media_list[-1]['timestamp']
                    logger.info(f"Current earliest media date: {earliest_date}")
            
            # Check each media item on this page
            for media in media_list:
                permalink = media.get('permalink', '')
                
                # Extract the media code from permalink
                extracted_code = None
                if '/p/' in permalink:
                    extracted_code = permalink.split('/p/')[1].split('/')[0]
                elif '/reel/' in permalink:
                    extracted_code = permalink.split('/reel/')[1].split('/')[0]
                    
                if extracted_code:
                    if page_count <= 1:  # Only print for the first page to avoid too much output
                        logger.debug(f"Checking media: {permalink}")
                    
                    if media_code == extracted_code:
                        media_id = media.get('id')
                        media_data = media
                        logger.info(f"Found matching media ID: {media_id}")
                        
                        # Check for collaboration data
                        if 'collaborators' in media:
                            logger.info(f"This post has collaborators: {media['collaborators']}")
                        if 'tagged_accounts' in media:
                            logger.info(f"This post has tagged accounts: {media['tagged_accounts']}")
                        if 'mentioned_profiles' in media:
                            logger.info(f"This post has mentioned profiles: {media['mentioned_profiles']}")
                        if 'branded_content_partner' in media:
                            logger.info(f"This post has branded content partners: {media['branded_content_partner']}")
                            
                        return media_id, media_data
            
            # Check if there's another page of results
            next_page = data.get('paging', {}).get('next')
        
        if not media_id:
            logger.warning(f"Could not find media with code {media_code} after checking {page_count} pages")
            logger.info(f"Time range of retrieved media: {earliest_date} to {latest_date}")
            
            # Try direct approach as a fallback
            logger.info("Trying direct approach...")
            try:
                oembed_url = f"{self.base_url}/{self.api_version}/instagram_oembed?url=https://www.instagram.com/reel/{media_code}/&access_token={access_token}"
                response = requests.get(oembed_url)
                if response.status_code == 200:
                    oembed_data = response.json()
                    logger.info(f"Found media via oembed: {oembed_data}")
                    # Unfortunately, oembed doesn't return the internal media ID we need
                else:
                    logger.error(f"Oembed approach failed: {response.text}")
            except Exception as e:
                logger.error(f"Error with oembed approach: {str(e)}")
                
            return None, None
    
    def get_instagram_comments(self, media_id, access_token, limit=20000):
        """
        Fetch comments from Instagram post with pagination support.
        """
        # Start with the first page of comments
        comments_url = f"{self.base_url}/{self.api_version}/{media_id}/comments?fields=id,text,timestamp,username,like_count,replies{{id,text,timestamp,username,like_count}}&limit=100&access_token={access_token}"
        
        all_comments = []
        next_page = comments_url
        page_count = 0
        total_comments = 0
        
        logger.info(f"Starting to retrieve comments for media ID: {media_id}")
        logger.info(f"Target limit: {limit} comments")
        
        # Loop through all pages of comments
        while next_page and (limit is None or total_comments < limit):
            page_count += 1
            logger.info(f"Retrieving comments page {page_count}")
            
            response = requests.get(next_page)
            if response.status_code != 200:
                logger.error(f"Error getting comments page {page_count}: {response.text}")
                break
            
            data = response.json()
            
            comments_page = data.get('data', [])
            
            if not comments_page:
                logger.info(f"No more comments found on page {page_count}")
                break
            
            # Process comments on this page
            comments_count = len(comments_page)
            total_comments += comments_count
            all_comments.extend(comments_page)
            
            logger.info(f"Retrieved {comments_count} comments from page {page_count} (total: {total_comments})")
            
            # Check for more pages of comments
            if 'paging' in data and 'next' in data['paging']:
                next_page = data['paging']['next']
                logger.debug(f"Found next page URL")
            else:
                logger.info("No more pages available (no 'next' link in response)")
                next_page = None
            
            # Add a small delay to avoid rate limiting
            if next_page:
                time.sleep(1)
        
        logger.info(f"Total top-level comments retrieved: {len(all_comments)}")
        
        # Now process replies
        logger.info("Starting to process replies for each comment...")
        comments_with_complete_replies = []
        total_replies = 0
        
        for comment_index, comment in enumerate(all_comments):
            # Debug info for this comment
            comment_id = comment.get('id')
            logger.debug(f"Processing comment {comment_index+1}/{len(all_comments)} (ID: {comment_id})")
            
            # Check if this comment has replies
            if 'replies' in comment:
                initial_replies = comment['replies'].get('data', [])
                logger.debug(f"  Comment has {len(initial_replies)} initial replies")
                
                # Check for pagination in replies
                has_more_replies = 'paging' in comment['replies'] and 'next' in comment['replies']['paging']
                
                if has_more_replies:
                    next_replies_url = comment['replies']['paging']['next']
                    logger.debug(f"  Comment has more replies.")
                    
                    # Get all replies for this comment
                    all_replies = list(initial_replies)  # Start with initial replies
                    replies_page_count = 1
                    
                    # Loop through all pages of replies for this comment
                    while next_replies_url and (limit is None or total_comments + total_replies < limit):
                        replies_page_count += 1
                        logger.debug(f"  Retrieving replies page {replies_page_count} for comment {comment_id}...")
                        
                        response = requests.get(next_replies_url)
                        if response.status_code != 200:
                            logger.error(f"  Error getting replies page {replies_page_count}: {response.text}")
                            break
                        
                        replies_data = response.json()
                        replies_page = replies_data.get('data', [])
                        
                        if not replies_page:
                            logger.debug(f"  No more replies found on page {replies_page_count}")
                            break
                        
                        # Add these replies
                        page_replies_count = len(replies_page)
                        all_replies.extend(replies_page)
                        total_replies += page_replies_count
                        
                        logger.debug(f"  Retrieved {page_replies_count} replies from page {replies_page_count}")
                        
                        # Check for more pages of replies
                        if 'paging' in replies_data and 'next' in replies_data['paging']:
                            next_replies_url = replies_data['paging']['next']
                            logger.debug(f"  Found next replies page")
                        else:
                            logger.debug("  No more reply pages available")
                            next_replies_url = None
                        
                        # Add a small delay to avoid rate limiting
                        if next_replies_url:
                            time.sleep(1)
                    
                    # Replace the original replies with the complete set
                    logger.debug(f"  Total replies for this comment: {len(all_replies)}")
                    comment['replies'] = {'data': all_replies}
                else:
                    logger.debug("  No additional reply pages for this comment")
                    total_replies += len(initial_replies)
            else:
                logger.debug("  Comment has no replies")
            
            comments_with_complete_replies.append(comment)
        
        logger.info(f"Final total: {total_comments} top-level comments and {total_replies} replies")
        logger.info(f"Total comments + replies: {total_comments + total_replies}")
        
        # Process comments into the required format
        output_comments = []
        
        for i, comment in enumerate(comments_with_complete_replies, 1):
            # Process main comment
            comment_date = comment.get('timestamp', '')
            username = comment.get('username', '')
            
            main_comment = {
                'id': i,
                'sub_id': '',
                'date': comment_date,
                'likes': comment.get('like_count', 0),
                'live_video_timestamp': '-',
                'comment': comment.get('text', ''),
                'image_urls': '',
                'view_source': 'view comment',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'client': '',  # Will be filled later
                'url': '',  # Will be filled later
                'platform': 'instagram',
                'week': '',  # Will be calculated later when we have the DataFrame
                'author': username
            }
            output_comments.append(main_comment)
            
            # Process replies if they exist
            if 'replies' in comment and 'data' in comment['replies']:
                for j, reply in enumerate(comment['replies']['data'], 1):
                    sub_id = f"{i}.{j}"
                    reply_date = reply.get('timestamp', '')
                    reply_username = reply.get('username', '')
                    
                    reply_comment = {
                        'id': i,
                        'sub_id': sub_id,
                        'date': reply_date,
                        'likes': reply.get('like_count', 0),
                        'live_video_timestamp': '-',
                        'comment': reply.get('text', ''),
                        'image_urls': '',
                        'view_source': 'view comment',
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'client': '',  # Will be filled later
                        'url': '',  # Will be filled later
                        'platform': 'instagram',
                        'week': '',  # Will be calculated later when we have the DataFrame
                        'author': reply_username
                    }
                    output_comments.append(reply_comment)
        
        return output_comments
    
    def extract_media_code_from_url(self, url):
        """
        Extract media code from Instagram URL.
        """
        # Handle post URLs
        if '/p/' in url:
            return url.split('/p/')[1].split('/')[0]
        # Handle reel URLs
        elif '/reel/' in url:
            return url.split('/reel/')[1].split('/')[0]
        # Try regex pattern for any other format
        else:
            pattern = r'instagram\.com/(?:p|reel)/([^/]+)'
            match = re.search(pattern, url)
            if match:
                return match.group(1)
            return None
    

    def process_link(self, link, access_token, client=None):
        """
        Process a single Instagram link.

        Args:
            link: Instagram post URL
            access_token: Facebook Graph API access token
            client: Client name (optional, can be a comma-separated list of names)

        Returns:
            List of formatted comments
        """
        logger.info(f"{'='*50}")
        logger.info(f"Processing Instagram post: {link}")
        if client:
            logger.info(f"Client: {client}")
        logger.info(f"{'='*50}")

        # Extract media code from URL
        media_code = self.extract_media_code_from_url(link)
        if not media_code:
            logger.error(f"Could not extract media code from URL: {link}")
            return []

        logger.info(f"Extracted media code: {media_code}")

        # Handle multiple mapped names
        if client:
            # Make sure client is a string before splitting
            if isinstance(client, str):
                mapped_names = [name.strip() for name in client.split(',')]
            else:
                # If client is not a string (e.g., None or a number)
                logger.warning(f"Client value is not a string: {client}, type: {type(client)}")
                mapped_names = []

            # Try each mapped name
            for name in mapped_names:
                if not name:  # Skip empty names
                    continue

                ig_business_id = self.get_instagram_business_id(access_token, name)
                if ig_business_id:
                    logger.info(f"Found Instagram Business ID for {name}: {ig_business_id}")
                    break  # Exit the loop if a valid ID is found
            else:
                logger.error(f"Failed to get Instagram Business ID for any mapped names: {mapped_names}.")
                return []
        else:
            logger.error("No client name provided, cannot get Instagram Business ID")
            return []

        # Find the media ID
        media_id, _ = self.search_instagram_media_with_extensive_pagination(ig_business_id, media_code, access_token)
        if not media_id:
            logger.error(f"Media not found. Skipping this post.")
            return []

        # Get comments for the media
        logger.info(f"Retrieving all comments for media ID: {media_id}")
        comments = self.get_instagram_comments(media_id, access_token)
        if not comments:
            logger.info(f"No comments found for this post.")
            return []

        # Add client and URL to all comments
        for comment in comments:
            comment['client'] = name  # Use the last valid name found
            comment['url'] = link

        # Add comments to our collection
        self.all_comments.extend(comments)
        logger.info(f"Added {len(comments)} comments. Total: {len(self.all_comments)}")

        return comments
    
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
            
            # Reorder columns to match the desired output layout
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
            import traceback
            traceback.print_exc()
            
            # Try to save in a different format as backup
            try:
                backup_path = f"instagram_comments_backup_{self.timestamp}.json"
                with open(backup_path, 'w') as f:
                    json.dump(self.all_comments, f)
                logger.info(f"Backup comments saved to {backup_path}")
                return backup_path
            except:
                logger.error("Failed to save backup")
                return None
                
