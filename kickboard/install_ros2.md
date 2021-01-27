# How to install ros2 on mac os
### ros requiers python 2.x
## 1. brew install
```
brew doctor
brew install python3
brew install asio tinyxml2
brew install tinyxml eigen pcre poco
brew install opencv
brew install openssl
brew install qt freetype assimp
brew install log4cxx
brew install sip pyqt5
```
## 2. fix some path and name
```
ln -s /usr/local/share/sip/Qt5 /usr/local/share/sip/PyQt5
brew install graphviz
python3 -m pip install pygraphviz pydot
python3 -m pip install lxml
python3 -m pip install catkin_pkg empy lark-parser pyparsing pyyaml setuptools argcomplete
python3 -m pip install lark
python3 -m pip install lark_parser
```
## 3. disable system integrity protection(SIP)
```
1. restart your mac
2. press option before apple logo is shown on your screen
3. click terminal at the menu bar, utilities -> terminal
4. csrutil status
5. if return say 'System Integrity Protection status: enabled.',
6. csrutil disable
7. else, just turn on your mac 
```
## 4. go to github and get resent mac folder
#### https://github.com/ros2/ros2/releases
#### latest package of mac and now I'm using ros2-eloquent-20201212-macos-amd64.tar.bz2
## 5. unpack it
#### use package which you installed at no.4
```
mkdir -p ~/ros2_crystal
cd ~/ros2_crystal
tar xf ~/Downloads/ros2-eloquent-20201212-macos-amd64.tar.bz2
```
## 6. environmental setup
### if you are using zsh, use zsh instead using bash
```
source ~/ros2_crystal/ros2-osx/setup.bash
```
## 7. try some examples
```
ros2 run demo_nodes_cpp talker
ros2 run demo_nodes_py listener
```
#### when bad interpreter: /usr/local/bin/python3: no such file or directory occur
```
1. check is /usr/local/bin/python3
```
##### if not
```
brew install python3
```
##### if exists
###### it occurs the error because user installed differnt version of pythons.  
###### which overwrite the pip3 from hombrew python3
###### we'll delete pip3 and re link pip and python along brew
```
rm /usr/local/bin/pip3
brew link python
```
#### try examples again if get some error/warnings
##### check here
```
https://index.ros.org/doc/ros2/Troubleshooting/Installation-Troubleshooting/#macos-troubleshooting
```
## 8. install end, here is tutorial of colcon
```
https://index.ros.org/doc/ros2/Tutorials/Colcon-Tutorial/
```
## how to uninstall ros2
```
rm -rf ~/ros2_crystal
```
# if python version doesn't correspond with ros2
## install python version at python.org
## check your python version
```
python --version
```
## check where your python is 
```
whcih python
```
#### my python is /usr/local/bin/python
## make link
### remember which python version was linked
```
ln -s -f /usr/local/bin/python{version whcih ros2 uses}  /usr/local/bin/python
```
## make venv
```
cd
mkdir venvs
python3 -m venv venvs/{your venv name}
```
## return python link
```
ln -s -f /usr/local/bin/python{version whcih you did remember}  /usr/local/bin/python
```
## activate venv
```
source ~/venvs/{your venv name}/bin/activate
```
## reinstall python library in your venv
```
python3 -m pip install pygraphviz pydot
python3 -m pip install lxml
python3 -m pip install catkin_pkg empy lark-parser pyparsing pyyaml setuptools argcomplete
python3 -m pip install lark
python3 -m pip install lark_parser
```
### to activate venv,
```
deactivate
```
### to remove venv,
```
(optional) deactivate
cd ~/venvs
rm -rf ./{your venv name}
```