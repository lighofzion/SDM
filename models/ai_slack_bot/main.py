import os
import re
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# Set environment variables for the tokens
SLACK_BOT_TOKEN = ""
SLACK_APP_TOKEN = ""

# Initialize the Slack app
app = App(token=SLACK_BOT_TOKEN)

# Define patterns and responses
# You can customize these patterns and responses as needed
RESPONSE_PATTERNS = [
    (r"(?i)hello|hi|hey", "Hello there! How can I help you today?"),
    (r"(?i)thanks|thank you", "You're welcome!"),
    (r"(?i)help", "I'm a bot that can help with various queries. What do you need assistance with?"),
    # Add more patterns as needed
]

# Handle regular messages
@app.event("message")
def handle_message_events(body, logger):
    # Ignore bot messages to prevent loops
    if "bot_id" in body.get("event", {}).get("bot_id"):
        return
    
    event = body["event"]
    channel_id = event["channel"]
    thread_ts = event.get("thread_ts", event.get("ts"))
    user_text = event.get("text", "")
    
    # Check if we should respond based on patterns
    response = get_response(user_text)
    
    if response:
        # Reply in thread
        app.client.chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,
            text=response
        )
        logger.info(f"Responded to '{user_text}' with '{response}'")

# Handle app mentions (@your-bot-name)
@app.event("app_mention")
def handle_app_mention_events(body, logger):
    event = body["event"]
    channel_id = event["channel"]
    thread_ts = event.get("thread_ts", event.get("ts"))
    user_text = event.get("text", "")
    
    # Remove the bot mention to process just the command
    clean_text = re.sub(r'<@[A-Z0-9]+>', '', user_text).strip()
    
    # Check if we should respond based on patterns
    response = get_response(clean_text) if clean_text else "How can I help you?"
    
    # Reply in thread
    app.client.chat_postMessage(
        channel=channel_id,
        thread_ts=thread_ts,
        text=response
    )
    logger.info(f"Responded to mention '{clean_text}' with '{response}'")

def get_response(text):
    """Generate a response based on the input text using regex patterns"""
    for pattern, response in RESPONSE_PATTERNS:
        if re.search(pattern, text):
            return response
    
    # Default response if no pattern matches
    return "I'm not sure how to respond to that. Could you provide more details?"

# Error handling
@app.error
def custom_error_handler(error, body, logger):
    logger.error(f"Error: {error}")
    logger.debug(f"Body: {body}")

# Start the app
if __name__ == "__main__":
    # Start the Socket Mode handler
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    print("⚡️ Slack bot is running!")
    handler.start()