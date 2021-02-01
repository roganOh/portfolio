# I used jetbot AI and jetson nano also I used macos so if you want to use
# Other os, use it and applicate this md file.
## 1. download jetbot image
#### goto https://github.com/NVIDIA-AI-IOT/jetbot/wiki/software-setup
#### click jetbot_image_?.zip in step 1
## 2. download balenaEther at https://www.balena.io/etcher/
## 3. do flashing your micro sd card with etcher
## insert os in jetson nano is finish
## 4. turn on your jetson with sd card
#### you'll need mouse, keyboard to connect with jetson
#### jetson can connect wifi and do bt transfertation 
## 5. install ROS Melodic
#### your jetson's pwd is 'jetson'
```
sudo apt-add-repository universe
sudo apt-add-repository multiverse
sudo apt-add-repository restricted
sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu $(lsb_release -sc) main" > /etc/apt/sources.list.d/ros-latest.list'
sudo apt-key adv --keyserver 'hkp://keyserver.ubuntu.com:80' --recv-key C1CF6E31E6BADE8868B172B4F42ED6FBAB17C654
sudo apt-get update
sudo apt-get install ros-melodic-ros-base
sudo sh -c 'echo "source /opt/ros/melodic/setup.bash" >> ~/.bashrc'
```
## close and restart the terminal
## 6. Install Adafruit Libraries
```
sudo apt-get install python-pip
pip install Adafruit-MotorHAT
pip install Adafruit-SSD1306
sudo usermod -aG i2c $USER
```
## reboot the system
## 7. Create catkin workspace
```
mkdir -p ~/workspace/catkin_ws/src
cd ~/workspace/catkin_ws
source /opt/ros/melodic/setup.bash
catkin_make
sudo sh -c 'echo "source ~/workspace/catkin_ws/devel/setup.bash" >> ~/.bashrc'
```
## 8. Build jetson-inference
### clone and build the jetson-inference repo
```
sudo apt-get install git cmake
cd ~/workspace
git clone https://github.com/dusty-nv/jetson-inference
cd jetson-inference
git submodule update --init
mkdir build
cd build
cmake ../
```
### some screen will pop up
### choose a/ models which you want to download
### at second screen, you can install PyTorch
```
make
sudo make install
```
## 9. Build ros_deep_learning
```
sudo apt-get install ros-melodic-vision-msgs ros-melodic-image-transport ros-melodic-image-publisher
cd ~/workspace/catkin_ws/src
git clone https://github.com/dusty-nv/ros_deep_learning
cd ~/workspace/catkin_ws
catkin_make
rospack find ros_deep_learning
```
## 10. Build jetbot_ros
```
cd ~/workspace/catkin_ws/src
git clone https://github.com/dusty-nv/jetbot_ros
cd ~/workspace/catkin_ws
catkin_make
rospack find jetbot_ros
```
## 11. Test jetbot
### open a new terminal and start roscore
```
roscore
```
### open a new terminal and test the jetbot
```
rosrun jetbot_ros jetbot_motors.py
```
### if a error that there's no module, try these
```
python2 -mpip install Adafruit-MotorHAT
python2 -mpip install Adafruit-SSD1306
```

###### https://m.blog.naver.com/PostView.nhn?blogId=zeta0807&logNo=222076575248&categoryNo=0&proxyReferer=https:%2F%2Fwww.google.com%2F
###### https://github.com/dusty-nv/jetbot_ros
