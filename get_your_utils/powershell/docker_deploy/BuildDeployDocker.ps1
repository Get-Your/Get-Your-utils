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

    ## Have the user select the deployment environment
    $EnvChoice = [System.Management.Automation.Host.ChoiceDescription[]](@(
        (New-Object System.Management.Automation.Host.ChoiceDescription("&Dev", "Development environment")),
        (New-Object System.Management.Automation.Host.ChoiceDescription("&Prod", "Production environment"))
    ))
    $EnvSelect = $Host.Ui.PromptForChoice("Environment", "Choose the environment to deploy to", $EnvChoice, 0)

    # Set the build tag based on the user's choice, with massaging
    $BuildTag = $EnvChoice[$EnvSelect].Label.Replace('&','').ToLower()

    $BuildStr = "$($env:DOCKER_ACCOUNT)/$($env:DOCKER_REPO):$($BuildTag)"

    ## Add the Git version as an env var
    # Run the Git command in the DEPLOY_DIR directory (-C flag)
    $CodeVersion = git -C $env:DEPLOY_DIR describe --tags

    ## Run the Docker build and push
    Write-Host "`nBuilding into $BuildStr..."
    docker build --build-arg CODE_VERSION=$CodeVersion -t $BuildStr $env:DEPLOY_DIR

    Write-Host "`nPushing to Docker hub..."
    docker push $BuildStr

    Read-Host -Prompt "Script complete (for $CodeVersion). Press any key to exit"

}
else {

    Read-Host -Prompt "Docker service did not start properly; script aborted. Press any key to exit"
}