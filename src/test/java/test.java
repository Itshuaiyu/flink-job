import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
    public static void main(String[] args) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX");
        System.out.println(df);
        SimpleDateFormat dfTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        System.out.println(dfTime);
        String format = df.format(new Date());
        String format1 = dfTime.format(new Date());
        System.out.println(format);
        System.out.println(format1);
    }
}
