package io.github.wirednerd.springbatch.document;

import javax.xml.bind.ValidationEventHandler;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static io.github.wirednerd.springbatch.document.JobExecutionDocumentMapper.ISO_DATE_PATTERN;

/**
 * Utility used to marshal and unmarshal Date objects in XML.
 *
 * @author Peter Busch
 */
public class DateXmlAdapter extends XmlAdapter<String, Date> {

    private transient final SimpleDateFormat format;

    /**
     * Utility used to marshal and unmarshal Date objects in XML.
     */
    public DateXmlAdapter() {
        super();
        format = new SimpleDateFormat(ISO_DATE_PATTERN); //NOPMD
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    /**
     * Convert a value type to a bound type.
     *
     * @param dateString The value to be converted. Can be null.
     * @throws ParseException if there's an error during the conversion. The caller is responsible for
     *                        reporting the error to the user through {@link ValidationEventHandler}.
     */
    @Override
    public Date unmarshal(String dateString) throws ParseException {
        return format.parse(dateString);
    }

    /**
     * Convert a bound type to a value type.
     *
     * @param date The value to be convereted. Can be null.
     */
    @Override
    public String marshal(Date date) {
        return format.format(date);
    }
}
