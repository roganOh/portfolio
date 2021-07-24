package vehicle;

public class AutomatedAuto extends AutomatedVehicle {

    public AutomatedAuto(VehicleType type, String owner, String make) {
        super(type, owner, make);
    }

    public void autoDrive() {
        System.out.println("Driving it myself");
    }

    public void autoPark() {
        System.out.println("Parking it myself");
    }

    public void makeNoise() {
        System.out.println("Beep, Beep");
    }

    public static void main(String[] args) {

        AutomatedAuto automatedCar =
                new AutomatedAuto(VehicleType.Automobile, "Martha", "Tesla");
        System.out.println(automatedCar);

        automatedCar.drive();
        automatedCar.park();
        automatedCar.makeNoise();
    }
}