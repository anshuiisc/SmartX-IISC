package query_engine;

import java.io.Serializable;

public class DataFormat implements Serializable {
    private long timeStamp;
    private float value;

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }
}