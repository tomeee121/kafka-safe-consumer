package TB.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.UUID;

@Data
public class Car {

    private String brand;
    private String model;
    private TB.model.UUID vin;

    public Car(@JsonProperty("brand") String brand, @JsonProperty("model") String model, @JsonProperty("vin") TB.model.UUID vin) {
        this.brand = brand;
        this.model = model;
        this.vin = vin;
    }
}
