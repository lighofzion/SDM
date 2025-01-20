const fs = require('fs');

const config = {
    SUPABASE_URL: process.env.SUPABASE_URL || '',
    SUPABASE_KEY: process.env.SUPABASE_KEY || ''
};

const configFileContent = `
window.SUPABASE_CONFIG = {
    SUPABASE_URL: "${config.SUPABASE_URL}",
    SUPABASE_KEY: "${config.SUPABASE_KEY}"
};
`;

fs.writeFileSync('config.js', configFileContent);
