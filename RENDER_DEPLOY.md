# Step-by-Step Guide to Deploying to Render.com

This guide will walk you through deploying your Flask application (with the pre-built Frontend) to Render.

## Prerequisites
1. A **GitHub** account with your code pushed to a repository.
2. A **Render.com** account.

---

## Step 1: Preparation (Already Done)
- Ensure your frontend is built (`npm run build`).
- Ensure the `dist` files are copied to `charted/app/static`.
- The `build.sh` and `requirements.txt` files are ready in the `charted` folder.

## Step 2: Create a PostgreSQL Database on Render
1. Log in to [Render Dashboard](https://dashboard.render.com/).
2. Click **New +** and select **PostgreSQL**.
3. **Name**: `sales-dashboard-db` (or anything you like).
4. **Plan**: Free (if available) or any other.
5. Click **Create Database**.
6. **Wait** for it to be "Available".
7. Scroll down to the **Connections** section and copy the **Internal Database URL**. It looks like `postgres://user:pass@host:port/db`. You will need this in Step 3.

## Step 3: Create a Web Service for the Flask App
1. Click **New +** and select **Web Service**.
2. Connect your GitHub repository.
3. **Name**: `sales-dashboard` (this will be part of your URL).
4. **Environment**: `Python 3`.
   - **Note**: I have added a `.python-version` file to the repo. Render will automatically use **Python 3.11**. If it asks, ensure you don't use 3.13.
5. **Region**: Choose one closest to you.
6. **Branch**: `main` (or your preferred branch).
7. **Root Directory**: (Leave this **BLANK**) if your repo contains `run.py` at the top level.  <-- **IMPORTANT**
8. **Build Command**: `chmod +x build.sh && ./build.sh`
9. **Start Command**: `gunicorn --bind 0.0.0.0:$PORT run:app`

## Step 4: Add Environment Variables
Before clicking "Create Web Service", scroll down or go to the **Environment** tab:
1. Click **Add Environment Variable**:
   - **Key**: `DATABASE_URL`
   - **Value**: (Paste the **Internal Database URL** you copied in Step 2).
2. Click **Add Environment Variable**:
   - **Key**: `SECRET_KEY`
   - **Value**: (Enter any long random string of text).
3. Click **Add Environment Variable**:
   - **Key**: `FLASK_ENV`
   - **Value**: `production`

## Step 5: Deploy
1. Click **Create Web Service**.
2. Render will now start the build process. You can see the logs in the "Events" or "Logs" tab.
3. Once the build finishes and says "Service is live", click the URL at the top to visit your site!

---

## Troubleshooting
- **Database Errors**: Double check that the `DATABASE_URL` is correct.
- **Login Issues**: The `build.sh` script automatically creates an admin user with username `admin` and password `admin36t`. You can use these to log in for the first time.
- **Static Files Not Loading**: Ensure the `static` folder contains your `index.html` and other assets.
