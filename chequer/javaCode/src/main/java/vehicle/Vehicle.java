package vehicle;
public abstract class Vehicle {

    protected enum VehicleType {
        Automobile, Motorcycle, Moped, Bicycle, Scooter
    }

    private VehicleType type;
    private String owner;
    private String make;

    public Vehicle(VehicleType type, String owner, String make) {
        this.type = type;
        this.owner = owner;
        this.make = make;
    }

    public String toString() {
        return "Vehicle{" +
                "type=" + type +
                ", owner='" + owner + '\'' +
                ", make='" + make + '\'' +
                '}';
    }

    public abstract void drive();

    public abstract void park();

    public abstract void makeNoise();
}