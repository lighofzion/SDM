export default function handler(req, res) {
    res.status(200).json({
      SUPABASE_URL: process.env.SUPABASE_URL,
      SUPABASE_KEY: process.env.SUPABASE_KEY
    });
  }
