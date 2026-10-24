# To run this script:
# powershell.exe -ExecutionPolicy Bypass -File "release.ps1"
#
# This script must only be run on the updated master branch.
#
# Note: For usage:
# 1. Ensure the github cli directory exists
# 2. Ensure git directory exists
# 3. Ensure python directory exists
# 4. Reference the GH token in an external file and reference it in this pipeline.
#
# Pipeline process:
# 1. Build the project to a binary to ensure that the project builds successfully
# 2. Update the version.json file to increment the patch version number (the third number in n.n.n)
# 3. Tag the master branch with the new version
# 4. Commit the updated version file and push it into the master branch
# 5. Build the project again (containing the new updated version.json file embedded in the binary)
# 6. Push the artefact as a release against the new version.

function GetGHToken{
    param([string]$TOKEN_FILE_PATH)
    if (-not(Test-Path $TOKEN_FILE_PATH -PathType Leaf)) {
        Write-Host("[ERROR] Cannot find $TOKEN_FILE_PATH. This is needed to get the Github Token")
        exit 1
    }
    $TOKEN_KEY_VALUE_STRING = Get-Content $TOKEN_FILE_PATH
    $KEY_VALUE_PAIR = $TOKEN_KEY_VALUE_STRING.Split('=')
    return $KEY_VALUE_PAIR[1]
}

# Tools setup
$TOKEN_FILE_PATH="$env:USERPROFILE\Documents\Development\code\deployment\scm-config.properties"
$GH_TOKEN = GetGHToken($TOKEN_FILE_PATH)
$env:GH_TOKEN=$GH_TOKEN
$PYTHON_HOME = "$env:USERPROFILE\AppData\Local\Programs\Python\Python310"
$PYTHON_EXE_PATH = "$PYTHON_HOME\python.exe"
$env:PYTHON_HOME = "$PYTHON_HOME"
$env:PYTHON_EXE_PATH = "$PYTHON_HOME"


$GIT_HOME = "C:\Program Files\Git"
$GIT_BIN = "$GIT_HOME\bin\git.exe"
$GITHUB_CLI_HOME = "C:\Program Files\GitHub CLI"
$GITHUB_CLI_BIN = "$GITHUB_CLI_HOME\gh.exe"

# Project Setup

$PROJECT_NAME = "data-ingestion"
$RELEASE_BINARY_NAME="DataIngestionRunner"
$PROJECT_HOME_DIRECTORY = "C:\Users\Dell\PycharmProjects\$PROJECT_NAME"
$RELEASE_FOLDER="$PROJECT_HOME_DIRECTORY/dist"
$RELEASE_BINARY_PATH="$RELEASE_FOLDER\$RELEASE_BINARY_NAME.exe"
$TEMP_FOLDER="$PROJECT_HOME_DIRECTORY/temp"

if ($GH_TOKEN -eq "") {
    Write-Host("[ERROR] Github token is required to run this release pipeline.")
    exit 1
}

if (-not(Test-Path $PYTHON_HOME -PathType Container)) {
    Write-Host "[ERROR] $PYTHON_HOME does not exist"
    exit 1
}


if (-not(Test-Path $PROJECT_HOME_DIRECTORY -PathType Container)) {
    Write-Host "[ERROR] $PROJECT_HOME_DIRECTORY does not exist"
    exit 1
}

if (-not(Test-Path $GIT_HOME -PathType Container)) {
    Write-Host "[ERROR] $GIT_HOME does not exist"
    exit 1
}

if (-not(Test-Path $GIT_BIN -PathType Leaf)) {
    Write-Host "[ERROR] $GIT_BIN does not exist"
    exit 1
}

if (-not(Test-Path $GITHUB_CLI_HOME -PathType Container)) {
    Write-Host "[ERROR] $GITHUB_CLI_HOME does not exist"
    exit 1
}

if (-not(Test-Path $GITHUB_CLI_BIN -PathType Leaf)) {
    Write-Host "[ERROR] $GITHUB_CLI_BIN does not exist"
    exit 1
}

cd $PROJECT_HOME_DIRECTORY

# run the pip commands
& pip install fastapi[standard]
& pip install mysql-connector-python
& pip install fastapi uvicorn python-multipart
& pip install pyinstaller


& pyinstaller --onefile --add-data="version.json;." --add-data="man-page.txt;." --add-data="api;api" --add-data="model;model" --add-data="controller;controller" --add-data="service;service" --hidden-import mysql.connector --hidden-import=uvicorn --hidden-import=fastapi --hidden-import=requests --hidden-import=main  DataIngestionRunner.py

IF ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Build failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}


# Here, write read the version manifest file in the client directory.
# Then read the version number

$VERSIONS_FILE = Join-Path $PROJECT_HOME_DIRECTORY "version.json"
$VERSIONS = Get-Content $VERSIONS_FILE -Raw | ConvertFrom-Json
$SERVICE_VERSION = $VERSIONS."version"

# The increment the minor version

# Split the version into parts
$PARTS = $SERVICE_VERSION.Split('.')

# Increment the last digit
$PARTS[2] = [int]$PARTS[2] + 1

 # Reassemble the version
$NEW_VERSION = $PARTS -join '.'

# Update the JSON property
$VERSIONS."version" = $NEW_VERSION

# Save the JSON back to the file
$VERSIONS | ConvertTo-Json -Depth 10 | Set-Content $VERSIONS_FILE

Write-Host "[INFO] Version updated from $SERVICE_VERSION to $NEW_VERSION"

# Take the new version and tag the git master branch with this new version

Write-Host "[INFO] Creating tag on branch ($NEW_VERSION)"
& $GIT_BIN tag -a $NEW_VERSION -m "Version $NEW_VERSION"

IF ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] An error occurred when trying to push to github.com: $LASTEXITCODE"
    exit $LASTEXITCODE
}

& $GIT_BIN push origin $NEW_VERSION

IF ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] An error occurred when trying to push to github.com: $LASTEXITCODE"
    exit $LASTEXITCODE
}

# Here, we need to commit the file version.json that has the up to date change log

git add version.json
Write-Host "[INFO] Committing updated version.json to repository"
& $GIT_BIN commit -m "Updating version.json change log with the new version ($NEW_VERSION)."
& $GIT_BIN push origin master

IF ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] An error occurred when trying to push to github.com: $LASTEXITCODE"
    exit $LASTEXITCODE
}

# now do an optimised release binary (exe) of the service.

& pyinstaller --onefile --add-data="version.json;." --add-data="man-page.txt;." --add-data="api;api" --add-data="model;model" --add-data="controller;controller" --add-data="service;service" --hidden-import mysql.connector --hidden-import=uvicorn --hidden-import=fastapi --hidden-import=requests --hidden-import=main  DataIngestionRunner.py


IF ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Optimised Release Build failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

# create the temp folder
New-Item -Path $TEMP_FOLDER -ItemType Directory -Force | Out-Null


# The zip the archive (below)

IF (Test-Path $RELEASE_FOLDER -PathType Container) {
    # if the release folder exists, then zip it up
    Compress-Archive -Path $RELEASE_FOLDER -DestinationPath $TEMP_FOLDER/$PROJECT_NAME -CompressionLevel Optimal -Force
} else {
    Write Host "[ERROR] $RELEASE_FOLDER does not exist. Exiting."
    exit 1
}

# Publish as a release against the version using the Github CLI token

$RELEASE_TITLE="$PROJECT_NAME Release ($NEW_VERSION)"
$RELEASE_NOTES="$PROJECT_NAME Release Notes ($NEW_VERSION)"

Write-Host "[INFO] Publishing artefact..."
& $GITHUB_CLI_BIN release create $NEW_VERSION "$TEMP_FOLDER/$PROJECT_NAME.zip" --notes $RELEASE_NOTES -t $RELEASE_TITLE
Write-Host ""
Write-Host "[INFO] Removing Temp Files"
# Commented the below line out as typically don't delete the dist folder. (It should be deleted, but kept in it for
# other reasons).
# Remove-Item "$RELEASE_FOLDER" -Recurse -Force
Remove-Item "$TEMP_FOLDER" -Recurse -Force
Write-Host ""
Write-Host "[INFO] Release Completed."