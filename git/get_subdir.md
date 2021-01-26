#how to get sub dir from git
### 1. make dir where you'll save files
### 2. init git
```
cd {to}/{your}/{dir}
git init
```
### 3. make can do sparse checkout
```
git config core.sparsecheckout true
```
### 4. add remote
```
git remote add -f {name} {remote_url}
```
### 5. add sparse checkout with new file
```
echo "{sub}/{dir}/{except}/{root}" >> .git/info/sparse-checkout
```
### 6. pull git subdir
```
git pull {name} {branch}
```