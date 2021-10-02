# git command for beginner
## git init
```
make dir to git dir
```
## git remote add (-f) {name} {url}
```
make dir can remote git
```
## git status
```
check what file does git tracking and which branch git is remoting
```
## git remote
```
check what is the name of remote
```
## git add {file}
```
make git track file
```
## git commit -m "{comment}"
```
commit git whith comment
```
## git push {name} {branch}
```
after commit, push to git
befor commit, files and dirs are not in git
```
## .gitignore
####.gititgnore must locate at root dir
```
# : comments

# no .a files
*.a

# but do track lib.a, even though you're ignoring .a files above
!lib.a

# only ignore the TODO file in the current directory, not subdir/TODO
/TODO

# ignore all files in the build/ directory
build/

# ignore doc/notes.txt, but not doc/server/arch.txt
doc/*.txt

# ignore all .pdf files in the doc/ directory
doc/**/*.pdf
```
## git rm -rf {file or folder}
```
delete from local and git
```
## git rm -r --cached {file or folder}
```
delete from only in remote
```

ssh
