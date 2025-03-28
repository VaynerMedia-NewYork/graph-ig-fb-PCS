import pandas as pd
import os
import warnings
from datetime import datetime
import time
import glob
import traceback

def cleanup_temp_files(ig_output_path, fb_output_path):
    """
    Clean up temporary files created during processing.
    
    Args:
        ig_output_path (str): Path to Instagram comments file to delete
        fb_output_path (str): Path to Facebook comments file to delete
    """
    try:
        # Remove individual platform files if they exist
        for file_path in [ig_output_path, fb_output_path]:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                print(f"Deleted temporary file: {file_path}")
        
        # Also look for any backup JSON files
        for backup_file in glob.glob("*_backup_*.json"):
            os.remove(backup_file)
            print(f"Deleted backup file: {backup_file}")
            
        print("Cleanup of temporary files completed")
    except Exception as e:
        print(f"Error during cleanup: {str(e)}")

def clear_output_directory(output_dir):
    """
    Clear the output directory of all files.
    
    Args:
        output_dir (str): Path to the output directory
    """
    try:
        # Get all files in the output directory
        files = [f for f in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, f))]
        
        # Remove all files
        for file in files:
            os.remove(os.path.join(output_dir, file))
            print(f"Deleted output file: {file}")
        
    except Exception as e:
        print(f"Error clearing output directory: {str(e)}")

def process_links(df, access_token, output_directory, graph_api_mapping):
    """
    Process all social media links in the dataframe.
    Handles both Instagram and Facebook links.
    
    Args:
        df (pandas.DataFrame): DataFrame containing 'client' and 'link' columns
        access_token (str): API access token for both Instagram and Facebook
        output_directory (str): Directory to save the output CSV file
        graph_api_mapping (dict): Mapping of client names to Graph API names
    
    Returns:
        tuple: (path to combined output CSV, DataFrame with comments)
    """
    try:
        # Import our fetchers 
        from .instagram.instagram_fetcher import InstagramFetcher
        from .facebook.facebook_fetcher import FacebookCommentsFetcher
        
        # Initialize fetchers
        ig_fetcher = InstagramFetcher()
        fb_fetcher = FacebookCommentsFetcher(access_token=access_token)
        
        # Create dataframes for each platform
        facebook_df = df[df['link'].str.contains('facebook', case=False, na=False)]
        instagram_df = df[df['link'].str.contains('instagram', case=False, na=False)]
        
        # Log counts
        print(f"Found {len(facebook_df)} Facebook links")
        print(f"Found {len(instagram_df)} Instagram links")
        
        # Track original client names and links
        link_to_client = dict(zip(df['link'], df['client']))
        
        # Process Instagram links
        print("Starting Instagram links processing...")
        for index, row in instagram_df.iterrows():
            try:
                client = row['client']
                link = row['link']
                mapped_names = graph_api_mapping.get(client, None)

                # Fall back to original client name if no mapping exists
                if mapped_names is None or pd.isna(mapped_names):
                    print(f"No mapping found for client '{client}', using original client name for fuzzy matching")
                    mapped_names = client  # Use the original client name
                else:
                    print(f"Mapped client '{client}' to Graph API name '{mapped_names}'")

                print(f"Processing Instagram link {index+1}/{len(instagram_df)}: {link}")
                comments = ig_fetcher.process_link(link, access_token, client=mapped_names)

                # Add a delay between requests to avoid rate limiting
                if index < len(instagram_df) - 1:
                    print("Waiting 3 seconds before next Instagram request...")
                    time.sleep(3)

            except Exception as e:
                print(f"Error processing Instagram link {row['link']}: {str(e)}")
                if not hasattr(ig_fetcher, 'failed_links'):
                    ig_fetcher.failed_links = []
                ig_fetcher.failed_links.append(row['link'])
                # Continue with next link
        
        # Process Facebook links
        print("Starting Facebook links processing...")
        for index, row in facebook_df.iterrows():
            try:
                client = row['client']
                link = row['link']
                mapped_names = graph_api_mapping.get(client, None)

                # Fall back to original client name if no mapping exists
                if mapped_names is None or pd.isna(mapped_names):
                    print(f"No mapping found for client '{client}', using original client name for fuzzy matching")
                    mapped_names = client  # Use the original client name
                else:
                    print(f"Mapped client '{client}' to Graph API name '{mapped_names}'")

                print(f"Processing Facebook link {index+1}/{len(facebook_df)}: {link}")
                comments = fb_fetcher.process_link(link, access_token, client=mapped_names)

                # Add a delay between requests to avoid rate limiting
                if index < len(facebook_df) - 1:
                    print("Waiting 3 seconds before next Facebook request...")
                    time.sleep(3)

            except Exception as e:
                print(f"Error processing Facebook link {row['link']}: {str(e)}")
                if not hasattr(fb_fetcher, 'failed_links'):
                    fb_fetcher.failed_links = []
                fb_fetcher.failed_links.append(row['link'])
                # Continue with next link
        
        # Save comments from both platforms
        print("Saving all comments data...")
        
        # Get comments and file paths from both fetchers
        ig_comments = getattr(ig_fetcher, 'all_comments', [])
        fb_comments = getattr(fb_fetcher, 'all_comments', [])
        
        # Store output paths for cleanup later
        ig_output_path = getattr(ig_fetcher, 'output_path', None)
        fb_output_path = getattr(fb_fetcher, 'output_path', None)
        
        # Prepare comments with original client names
        prepared_comments = []
        for comment in (ig_comments + fb_comments):
            # Find the original client name based on the URL
            original_client = None
            for link, client in link_to_client.items():
                if comment.get('url') and link in comment['url']:
                    original_client = client
                    break
            
            # If no link match found, try to use the current client name
            if original_client is None:
                original_client = comment.get('client', 'Unknown')
            
            # Update the comment with the original client name
            comment['client'] = original_client
            prepared_comments.append(comment)
        
        # Combine all comments
        if not prepared_comments:
            print("No comments were collected from any platform.")
            return None, None
        
        # Create timestamp for combined file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        combined_output_path = os.path.join(output_directory, f"all_social_comments_{timestamp}.csv")
        
        # Create DataFrame from combined comments
        print(f"Creating combined DataFrame with {len(prepared_comments)} comments...")
        comments_df = pd.DataFrame(prepared_comments)
        
        # Process the data
        print("Processing dates and adding week column...")
        comments_df['date'] = pd.to_datetime(comments_df['date'])
        comments_df['week'] = comments_df['date'] - pd.to_timedelta(comments_df['date'].dt.weekday, unit='D')
        comments_df['week'] = comments_df['week'].dt.strftime('%Y-%m-%d')
        
        # Reorder columns to match the desired output layout
        ordered_columns = [
            'id', 'sub_id', 'date', 'week', 'likes', 'live_video_timestamp',
            'comment', 'image_urls', 'view_source', 'timestamp',
            'client', 'url', 'platform'
        ]
        
        # Filter columns to only those we have
        ordered_columns = [col for col in ordered_columns if col in comments_df.columns]
        comments_df = comments_df[ordered_columns]
        
        # Save to CSV
        print(f"Saving {len(comments_df)} comments to {combined_output_path}")
        comments_df.to_csv(combined_output_path, index=False)
        print(f"All social media comments saved to {combined_output_path}")
        
        # Clean up temporary files after successful combined file creation
        print("Cleaning up temporary files...")
        cleanup_temp_files(ig_output_path, fb_output_path)
        
        # Retry failed links
        print("Retrying failed links...")
        failed_links = getattr(ig_fetcher, 'failed_links', []) + getattr(fb_fetcher, 'failed_links', [])
        if failed_links:
            for link in failed_links:
                try:
                    # Find the original client name for this link
                    client_name = link_to_client.get(link, '')
                    mapped_names = graph_api_mapping.get(client_name, None)
                    
                    if mapped_names is None or pd.isna(mapped_names):
                        mapped_names = client_name
                    else:
                        print(f"Retry: Mapped client '{client_name}' to Graph API name '{mapped_names}'")
                    
                    if 'instagram' in link.lower():
                        comments = ig_fetcher.process_link(link, access_token, client=mapped_names)
                    elif 'facebook' in link.lower():
                        comments = fb_fetcher.process_link(link, access_token, client=mapped_names)
                    
                    # Prepare retried comments with original client name
                    if comments:
                        for comment in comments:
                            comment['client'] = client_name
                        comments_df = pd.concat([comments_df, pd.DataFrame(comments)], ignore_index=True)
                    
                    print(f"Retried link: {link}")
                except Exception as e:
                    print(f"Error retrying link {link}: {str(e)}")
        
        # Final save with retried links
        if not comments_df.empty:
            # Reapply processing to new DataFrame
            comments_df['date'] = pd.to_datetime(comments_df['date'])
            comments_df['week'] = comments_df['date'] - pd.to_timedelta(comments_df['date'].dt.weekday, unit='D')
            comments_df['week'] = comments_df['week'].dt.strftime('%Y-%m-%d')
            comments_df.to_csv(combined_output_path, index=False)
            print(f"Updated comments file with retried links: {combined_output_path}")
        
        return combined_output_path, comments_df
        
    except Exception as e:
        print(f"Error in process_links function: {str(e)}")
        traceback.print_exc()
        return None, None

# CHANGE

def main(df, access_token, graph_api_mapping):
    """
    Main function to process all Facebook and Instagram links and save comments to a CSV file.
    
    Args:
        df (pandas.DataFrame): DataFrame with social media links to process
        access_token (str): API access token for both Instagram and Facebook
        graph_api_mapping (dict): Mapping of client names to their corresponding graph API names
        
    Returns:
        tuple: (path to output file, DataFrame with comments)
    """
    try:
        # Define output directory
        output_directory = 'output'
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
            print(f"Created output directory: {output_directory}")
        
        # Clear output directory at the start
        clear_output_directory(output_directory)
        
        # Check if we have data
        if df.empty:
            print("No data available for processing.")
            return None, None
        
        # Process all links and save results
        output_file, comments_df = process_links(df, access_token, output_directory, graph_api_mapping)
        
        if output_file:
            print(f"Processing completed successfully. Results saved to {output_file}")
        else:
            print("Processing completed with errors. No output file was created.")
            
        return output_file, comments_df
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None
