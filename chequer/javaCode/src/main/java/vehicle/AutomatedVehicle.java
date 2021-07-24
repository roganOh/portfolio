package vehicle;

public abstract class AutomatedVehicle extends Vehicle {

    public AutomatedVehicle(VehicleType type, String owner, String make) {
        super(type, owner, make);
    }

    public void park() {
        autoPark();
    }

    public void drive() {
        autoDrive();
    }

    abstract void autoPark();

    abstract void autoDrive();
}