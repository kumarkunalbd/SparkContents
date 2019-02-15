import java.io.Serializable;

public class StockData implements Serializable{
    private String symbol;
    private String timestamp;
    private PriceData priceData;

    public String getSymbol() {
    return symbol;
    }

    public void setSymbol(String symbol) {
    this.symbol = symbol;
    }

    public String getTimestamp() {
    return timestamp;
    }

    public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
    }

    public PriceData getPriceData() {
    return priceData;
    }

    public void setPriceData(PriceData priceData) {
    this.priceData = priceData;
    }

    public class PriceData implements Serializable{
        private String open;
        private String high;
        private String low;
        private String close;
        private String volume;

        public String getOpen() {
        return open;
        }

        public void setOpen(String open) {
        this.open = open;
        }

        public String getHigh() {
        return high;
        }

        public void setHigh(String high) {
        this.high = high;
        }

        public String getLow() {
        return low;
        }

        public void setLow(String low) {
        this.low = low;
        }

        public String getClose() {
        return close;
        }

        public void setClose(String close) {
        this.close = close;
        }

        public String getVolume() {
        return volume;
        }

        public void setVolume(String volume) {
        this.volume = volume;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("PriceData [open=");
            builder.append(open);
            builder.append(", high=");
            builder.append(high);
            builder.append(", low=");
            builder.append(low);
            builder.append(", close=");
            builder.append(close);
            builder.append(", volume=");
            builder.append(volume);
            builder.append("]");
            return builder.toString();
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("StockData [symbol=");
        builder.append(symbol);
        builder.append(", timestamp=");
        builder.append(timestamp);
        builder.append(", priceData=");
        builder.append(priceData);
        builder.append("]");
        return builder.toString();
    }

    public String PrintStockSymbol() {
        return symbol + " ," ;
    }
}
