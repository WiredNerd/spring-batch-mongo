package io.github.wirednerd.springbatch.document;

import javax.xml.bind.ValidationEventHandler;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper.ISO_DATE_PATTERN;

public class DateXmlAdapter extends XmlAdapter<String, Date> {

    private final SimpleDateFormat format;

    public DateXmlAdapter() {
        super();
        format = new SimpleDateFormat(ISO_DATE_PATTERN);
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    /**
     * Convert a value type to a bound type.
     *
     * @param dateString The value to be converted. Can be null.
     * @throws Exception if there's an error during the conversion. The caller is responsible for
     *                   reporting the error to the user through {@link ValidationEventHandler}.
     */
    @Override
    public Date unmarshal(String dateString) throws Exception {
        return format.parse(dateString);
    }

    /**
     * Convert a bound type to a value type.
     *
     * @param date The value to be convereted. Can be null.
     * @throws Exception if there's an error during the conversion. The caller is responsible for
     *                   reporting the error to the user through {@link ValidationEventHandler}.
     */
    @Override
    public String marshal(Date date) throws Exception {
        return format.format(date);
    }
}
