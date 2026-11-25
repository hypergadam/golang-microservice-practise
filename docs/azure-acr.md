# Pushing images to Azure Container Registry

The workflow [`.github/workflows/azure-acr.yml`](../.github/workflows/azure-acr.yml) builds the
`cart`, `stocks` and `metrics-consumer` images and pushes them to Azure Container Registry (ACR).
It runs on every push to `main` that touches a service, and can be started manually from the
Actions tab.

GitHub logs in to Azure with **OIDC**: Azure trusts tokens that GitHub issues for this repository's
`main` branch, so no Azure password or secret is stored in GitHub.

Images are tagged with the commit SHA and `latest`, e.g. `<acr>.azurecr.io/cart:<sha>`.

## One-time setup

You need the [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli).

### 1. Log in and pick names

```bash
az login

RG=microservices-rg
LOCATION=westeurope            # Azure for Students may restrict regions; pick an allowed one
ACR=hypergadamshop$RANDOM      # globally unique, lowercase letters and digits only
REPO=hypergadam/golang-microservice-practise
```

### 2. Create the resource group and registry

```bash
az group create --name $RG --location $LOCATION
az acr create --name $ACR --resource-group $RG --sku Basic
```

The Basic tier costs about $5/month and is paid from the Azure for Students credit.

### 3. Create an identity for GitHub Actions

A user-assigned managed identity is used instead of an app registration, because student
accounts often aren't allowed to create app registrations in their school's directory.

```bash
az identity create --name github-acr-push --resource-group $RG

CLIENT_ID=$(az identity show -n github-acr-push -g $RG --query clientId -o tsv)
PRINCIPAL_ID=$(az identity show -n github-acr-push -g $RG --query principalId -o tsv)

az identity federated-credential create \
  --name github-main \
  --identity-name github-acr-push \
  --resource-group $RG \
  --issuer https://token.actions.githubusercontent.com \
  --subject "repo:$REPO:environment:azure" \
  --audiences api://AzureADTokenExchange
```

The workflow job runs in the GitHub environment `azure`, so the token subject is
`repo:<owner>/<repo>:environment:azure`. Only jobs in that environment can log in.

### 4. Give the identity access

```bash
RG_ID=$(az group show -n $RG --query id -o tsv)
ACR_ID=$(az acr show -n $ACR --query id -o tsv)

# Reader on the resource group: lets `az login` and `az acr login` find the registry
az role assignment create --assignee-object-id $PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal --role Reader --scope $RG_ID

# AcrPush on the registry: lets it push images
az role assignment create --assignee-object-id $PRINCIPAL_ID \
  --assignee-principal-type ServicePrincipal --role AcrPush --scope $ACR_ID
```

### 5. Add the values to GitHub

None of these are secrets, so they're stored as environment **variables**:

```bash
echo "AZURE_CLIENT_ID=$CLIENT_ID"
echo "AZURE_TENANT_ID=$(az account show --query tenantId -o tsv)"
echo "AZURE_SUBSCRIPTION_ID=$(az account show --query id -o tsv)"
echo "ACR_NAME=$ACR"
```

In GitHub: **Settings → Environments → New environment** named `azure`, then add the four values
under **Environment variables**. Or with the GitHub CLI:

```bash
gh api -X PUT repos/$REPO/environments/azure
gh variable set AZURE_CLIENT_ID       --env azure --body "$CLIENT_ID"
gh variable set AZURE_TENANT_ID       --env azure --body "$(az account show --query tenantId -o tsv)"
gh variable set AZURE_SUBSCRIPTION_ID --env azure --body "$(az account show --query id -o tsv)"
gh variable set ACR_NAME              --env azure --body "$ACR"
```

### 6. Run it

Push to `main`, or start **Actions → Push images to Azure Container Registry → Run workflow**.
Check the result:

```bash
az acr repository list --name $ACR -o table
az acr repository show-tags --name $ACR --repository cart -o table
```

## Cost control

- Set a budget alert: **Cost Management → Budgets** in the Azure portal.
- To remove everything: `az group delete --name $RG` (deletes the registry and the identity).
