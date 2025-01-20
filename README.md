# Connection Status Website

This project is a static website that displays the connection status to Supabase. It checks whether the connection to the Supabase API is successful and updates the UI accordingly.

## Project Structure

- `src/index.html`: The main HTML document that structures the UI.
- `src/css/styles.css`: Contains styles for the website.
- `src/js/main.js`: JavaScript code for checking the connection status.
- `src/utils/supabase.js`: Utility functions for interacting with the Supabase API.
- `package.json`: Configuration file for npm.

## Setup

1. Clone the repository:
   ```
   git clone <repository-url>
   ```

2. Navigate to the project directory:
   ```
   cd connection-status-website
   ```

3. Install dependencies:
   ```
   npm install
   ```

4. Open `src/index.html` in a web browser to view the application.

## Usage

The website will automatically check the connection to Supabase when loaded. The connection status will be displayed on the UI.