# This script will build and deploy the Docker container to the account specified in .env

# Notes and caveats:
    # Docker must be installed on the system and have been run manually to store credentials
    # This script does not handle secrets!

    # This will build/deploy whatever branch and state manually set in Git

    # The .env file for this script cannot have '=' in the values, and the values should not be surrounded in quotes

## Wait for Docker service to start (if the machine has just rebooted)
Write-Host "Waiting for Docker service..."
$proc = Start-Process -PassThru -Wait docker info
if ( $proc.ExitCode -eq 0 ) {

    Write-Host "Docker service ready"

    ## Initialize vars
    ## Set environment variables from the .env file
    Get-Content $(Join-Path $pwd ".env") | ForEach-Object {
        $name, $value = $_.split('=')
        Set-Content env:\$name $value
    }

    $BuildStr = "$($env:DOCKER_ACCOUNT)/$($env:DOCKER_REPO):$($env:BUILD_TAG)"

    ## Add the Git version file (to be removed after build)
    # Run the Git command in the DEPLOY_DIR directory (-C flag)
    git -C $env:DEPLOY_DIR describe --tags | Out-File -Encoding utf8 $(Join-Path $env:DEPLOY_DIR ".gitversion")

    ## Run the Docker build and push
    Write-Host "`nBuilding into $BuildStr..."
    docker build -t $BuildStr $env:DEPLOY_DIR

    Write-Host "`nPushing to Docker hub..."
    docker push $BuildStr

    ## Remove the Git version file (it's to be used within a Docker build only)
    Remove-Item $(Join-Path $env:DEPLOY_DIR ".gitversion")

    Read-Host -Prompt "Script complete. Press any key to exit"

}
else {

    Read-Host -Prompt "Docker service did not start properly; script aborted. Press any key to exit"
}