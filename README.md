# Instagram Comments Retrieval System Documentation

## Overview

This system retrieves comments from Instagram posts using the Facebook Graph API. It processes a list of Instagram posts from various clients, extracts all comments (including nested replies), and saves them to a structured CSV file.

## Table of Contents

1. [Data Flow](#data-flow)
2. [Key Functions](#key-functions)
3. [Output Format](#output-format)
4. [Required Permissions](#required-permissions)
5. [Rate Limiting Considerations](#rate-limiting-considerations)
6. [Handling Collaborative Posts](#handling-collaborative-posts)
7. [Troubleshooting](#troubleshooting)

## Data Flow

1. **Input**: DataFrame with client names and Instagram post URLs
2. **Processing Pipeline**:
   - Extract media codes from URLs
   - Get Instagram Business Account IDs
   - Retrieve media IDs via pagination
   - Extract all comments and replies using pagination
   - Format comments into the desired structure
3. **Output**: CSV file with structured comment data

## Key Functions

### `main(df, access_token)`

The entry point for the script that orchestrates the entire process:

- Takes a DataFrame of Instagram posts and an access token
- Calls `process_instagram_posts` to retrieve comments
- Saves results to a CSV file

### `process_instagram_posts(df, access_token)`

Processes each Instagram post in the DataFrame:

- Iterates through each row in the input DataFrame
- For each post:
  1. Extracts the media code from the URL
  2. Gets the Instagram Business Account ID for the client
  3. Searches for the corresponding media ID
  4. Retrieves all comments and replies
  5. Adds client and URL information to each comment
- Returns a DataFrame with all comments from all posts

### `get_instagram_business_id(access_token, page_name)`

Maps a client name to an Instagram Business Account ID:

- Searches for Facebook Pages that match the client name
- Uses fuzzy matching if exact match is not found
- Retrieves the connected Instagram Business Account ID

### `search_instagram_media_with_extensive_pagination(ig_business_id, media_code, access_token)`

Finds the internal media ID for an Instagram post:

- Uses pagination to search through all media for a specific account
- Extracts and compares media codes from permalinks
- Returns the internal media ID needed for comment retrieval

### `get_instagram_comments_debug(media_id, access_token, limit=30000)`

Retrieves all comments and replies for a specific media ID:

- Handles pagination for both top-level comments and replies
- Processes nested comments (replies) for each comment
- Formats comments into the required output structure
- Includes detailed logging of the pagination process

### `extract_media_code_from_url(url)`

Extracts the media code from an Instagram URL:

- Handles both post (`/p/`) and reel (`/reel/`) URL formats
- Returns the unique media code used in API requests

## Output Format

The system generates a CSV file with the following columns:

| Column | Description |
|--------|-------------|
| id | Sequential ID for each main comment |
| sub_id | ID for nested replies (e.g., "1.1" for first reply to comment #1) |
| date | Timestamp from the Instagram comment |
| likes | Number of likes on the comment |
| live_video_timestamp | Set to "-" for Instagram comments |
| comment | The actual comment text |
| image_urls | Any image URLs in the comment (usually empty) |
| view_source | Set to "view comment" |
| timestamp | When the comment was processed |
| client | The client name from the input DataFrame |
| url | The Instagram post URL |
| platform | Set to "instagram" |
| week | monday of the post date week |

Example output row:
```
1.0,,2023-05-15T14:32:10+0000,12,-,Great post!,,view comment,2023-05-16 09:45:23,LIV Golf,https://www.instagram.com/p/Cx1rYZ5rxKQ/,instagram
```

## Required Permissions

To use this system, your access token needs these permissions:

- `instagram_basic` - For basic Instagram data access
- `instagram_manage_comments` - For reading comments
- `pages_read_engagement` - For accessing engagement data
- `pages_show_list` - For listing connected pages

## Rate Limiting Considerations

The system implements several strategies to handle API rate limits:

1. **Delay Between Requests**: 1-second delay between pagination requests
2. **Delay Between Posts**: 5-second delay between processing different posts
3. **Proper Error Handling**: Catches and logs rate limit errors

## API Limitations

Key API Limitations:
1. Partner/Collaborated Posts: Comments cannot be pulled via API - the post isn't owed by the client business page.
2. GIFs in Comments: Not provided by API under any circumstances; API only returns metadata (ID, media_product_type) without GIF content.
3. Partial Comment Retrieval (40-60%):
GIFs Issue: For posts like instagram.com/reel/DHM2bF1Oexm with 279 vs 121 comments retrieved, manual verification found 151 GIFs, confirming API limitation.
Privacy Filters: For brand accounts like Jimmy Johns (instagram.com/p/DGwGmJYJqz5 with 420 vs 298 comments), comments are likely filtered due to:
Hidden comments (inappropriate language, selected keywords) by client account based on an auto detect feature
User cookie settings

## Troubleshooting

### Common Issues

1. **"Failed to get Instagram Business ID"**
   - Ensure the client name matches a Facebook Page you have access to
   - Check that your access token has the required permissions

2. **"Could not find media with code"**
   - Verify the Instagram URL is correct
   - The post might be too old or deleted

3. **"Error getting comments page"**
   - You might have hit a rate limit - wait and try again
   - Check your access token hasn't expired

### Debugging Tips

The script includes extensive logging:

- Each API request URL is logged
- Pagination information is displayed
- Comment counts for each page are shown
- Error responses are printed in full

For additional debugging, look for these log messages:

- "First page response keys" - Shows available data from the API
- "Paging info" - Shows pagination structure
- "Retrieved X comments from page Y" - Tracks progress

---

This documentation provides a comprehensive overview of the Instagram Comments Retrieval System. The system is designed to efficiently retrieve comments from multiple Instagram posts while handling pagination, rate limiting, and proper data formatting.
