/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cli;

import com.facebook.presto.cli.ClientOptions.OutputFormat;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.airlift.log.Logger;

import org.fusesource.jansi.Ansi;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.cli.ConsolePrinter.REAL_TERMINAL;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class Query
        implements Closeable
{
    private static final Logger log = Logger.get(Query.class);

    private static final Signal SIGINT = new Signal("INT");

    private final AtomicBoolean ignoreUserInterrupt = new AtomicBoolean();
    private final StatementClient client;

    public Query(StatementClient client)
    {
        this.client = checkNotNull(client, "client is null");
    }

    public void renderOutput(PrintStream out, OutputFormat outputFormat, boolean interactive, String filter_with)
    {
        SignalHandler oldHandler = Signal.handle(SIGINT, new SignalHandler()
        {
            @Override
            public void handle(Signal signal)
            {
                if (ignoreUserInterrupt.get() || client.isClosed()) {
                    return;
                }
                try {
                    if (!client.cancelLeafStage()) {
                        client.close();
                    }
                }
                catch (RuntimeException e) {
                    log.debug(e, "error canceling leaf stage");
                    client.close();
                }
            }
        });
        try {
            renderQueryOutput(out, outputFormat, interactive, filter_with);
        }
        finally {
            Signal.handle(SIGINT, oldHandler);
        }
    }

    private void renderQueryOutput(PrintStream out, OutputFormat outputFormat, boolean interactive, String filter_with)
    {
        StatusPrinter statusPrinter = null;
        @SuppressWarnings("resource")
        PrintStream errorChannel = interactive ? out : System.err;

        if (interactive) {
            statusPrinter = new StatusPrinter(client, out);
            statusPrinter.printInitialStatusUpdates();
        }
        else {
            waitForData();
        }

        if ((!client.isFailed()) && (!client.isGone()) && (!client.isClosed())) {
            QueryResults results = client.isValid() ? client.current() : client.finalResults();
            if (results.getColumns() == null) {
                errorChannel.printf("Query %s has no columns\n", results.getId());
                return;
            }

            try {
                renderResults(out, outputFormat, interactive, results, filter_with);
            }
            catch (QueryAbortedException e) {
                System.out.println("(query aborted by user)");
                client.close();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        if (statusPrinter != null) {
            statusPrinter.printFinalInfo();
        }

        if (client.isClosed()) {
            errorChannel.println("Query aborted by user");
        }
        else if (client.isGone()) {
            errorChannel.println("Query is gone (server restarted?)");
        }
        else if (client.isFailed()) {
            renderFailure(client.finalResults(), errorChannel);
        }
    }

    private void waitForData()
    {
        while (client.isValid() && (client.current().getData() == null)) {
            client.advance();
        }
    }

    private void renderResults(PrintStream out, OutputFormat format, boolean interactive, QueryResults results, String filter_with)
            throws IOException
    {
        List<String> fieldNames = Lists.transform(results.getColumns(), Column.nameGetter());
        HashMap<Integer,Tuple.Triple<String,String, String>> filterMap = new HashMap<Integer,Tuple.Triple<String,String,String>>();
       
        String filterKeys = null;
        String[] filterItems = null;
        
        if(filter_with !=null && filter_with.contains(Constants.SEMI_COLON))
        	filterKeys = filter_with.split(Constants.SEMI_COLON)[0].trim();
        else {
        	System.out.println("FILTER_WITH command should end with a semi-colon.");
        	filterMap = null;
        	filterKeys = null;
        	filterItems = null;
        	return;
        }
        
        if(filterKeys !=null && filterKeys.contains(Constants.COMMA))
        	filterItems = filterKeys.split(Constants.COMMA);
        else {
        	if(filterKeys !=null && (filterKeys.contains(Constants.EQ) || filterKeys.contains(Constants.GT) || filterKeys.contains(Constants.LT))) 
        		filterItems = new String[] {filterKeys.trim()};
        }
       
        if(filterItems !=null && filterItems.length > 0) {
        	for(String item : filterItems) {
        		String attributeName = null;
        		String attributeCondition = null;
        		
        		int attributeIndex = -1;
        		
        		if(item.contains(Constants.DOT)) {
        			attributeName = item.split("\\.")[0];
        			attributeCondition = item.split("\\.")[1];
        		}
        			
        		if(attributeName != null)	
        			attributeIndex = fieldNames.indexOf(attributeName.trim());
        		
        		if(attributeIndex !=-1) {
        			if(attributeCondition != null && attributeCondition.contains(Constants.EQ)) {
        				Tuple.Triple<String, String, String> conditionMap = new Tuple.Triple<String,String, String> (attributeCondition.split(Constants.EQ)[0].trim(),Constants.EQ,attributeCondition.split(Constants.EQ)[1].trim());
        				filterMap.put(attributeIndex, conditionMap);
        				conditionMap = null;
        			}
        			else if(attributeCondition != null && attributeCondition.contains(Constants.GT)) {
        				Tuple.Triple<String, String, String> conditionMap = new Tuple.Triple<String, String, String>(attributeCondition.split(Constants.GT)[0].trim(),Constants.GT,attributeCondition.split(Constants.GT)[1].trim());
        				filterMap.put(attributeIndex, conditionMap);
        				conditionMap = null;
        			} 
        			else if(attributeCondition != null && attributeCondition.contains(Constants.LT)) {
        				Tuple.Triple<String, String, String> conditionMap = new Tuple.Triple<String, String, String>(attributeCondition.split(Constants.LT)[0].trim(),Constants.LT,attributeCondition.split(Constants.LT)[1].trim());
        				filterMap.put(attributeIndex, conditionMap);
        				conditionMap = null;
        			}
        			else {
        				System.out.println("One or more attribute conditions(s) in the FILTER_WITH section is incorrect - check query.");
            			filterMap = null;
            			filterKeys = null;
                    	filterItems = null;
                    	attributeName = null;
                    	attributeCondition = null;
                    	return;
        			}
        			
        		} else {
        			System.out.println("One or more attribute name(s) in the FILTER_WITH section is incorrect - check for Upper case, white spaces etc.");
        			filterMap = null;
                	filterKeys = null;
                	filterItems = null;
                	attributeName = null;
                	attributeCondition = null;
                	return;
        		}
        	}
        }
        
        if (interactive) {
            pageOutput(format, fieldNames, filterMap);
        }
        else {
            sendOutput(out, format, fieldNames, filterMap);
        }
    }

    private void pageOutput(OutputFormat format, List<String> fieldNames, HashMap<Integer,Tuple.Triple<String,String, String>> filterMap)
            throws IOException
    {
        // ignore the user pressing ctrl-C while in the pager
        ignoreUserInterrupt.set(true);

        try (Writer writer = createWriter(Pager.create());
                OutputHandler handler = createOutputHandler(format, writer, fieldNames)) {
            handler.processRows(client, filterMap);
        }
    }

    private void sendOutput(PrintStream out, OutputFormat format, List<String> fieldNames, HashMap<Integer,Tuple.Triple<String,String, String>> filterMap)
            throws IOException
    {
        try (OutputHandler handler = createOutputHandler(format, createWriter(out), fieldNames)) {
            handler.processRows(client, filterMap);
        }
    }

    private static OutputHandler createOutputHandler(OutputFormat format, Writer writer, List<String> fieldNames)
    {
        return new OutputHandler(createOutputPrinter(format, writer, fieldNames));
    }

    private static OutputPrinter createOutputPrinter(OutputFormat format, Writer writer, List<String> fieldNames)
    {
        switch (format) {
            case ALIGNED:
                return new AlignedTuplePrinter(fieldNames, writer);
            case VERTICAL:
                return new VerticalTuplePrinter(fieldNames, writer);
            case CSV:
                return new CsvPrinter(fieldNames, writer, false);
            case CSV_HEADER:
                return new CsvPrinter(fieldNames, writer, true);
            case TSV:
                return new TsvPrinter(fieldNames, writer, false);
            case TSV_HEADER:
                return new TsvPrinter(fieldNames, writer, true);
        }
        throw new RuntimeException(format + " not supported");
    }

    private static Writer createWriter(OutputStream out)
    {
        return new OutputStreamWriter(out, Charsets.UTF_8);
    }

    @Override
    public void close()
    {
        client.close();
    }

    public void renderFailure(QueryResults results, PrintStream out)
    {
        out.printf("Query %s failed: %s%n", results.getId(), results.getError().getMessage());
        if (client.isDebug()) {
            renderStack(results, out);
        }
        renderErrorLocation(client.getQuery(), results, out);
    }

    private static void renderErrorLocation(String query, QueryResults results, PrintStream out)
    {
        if (results.getError().getErrorLocation() != null) {
            renderErrorLocation(query, results.getError().getErrorLocation(), out);
        }
    }

    private static void renderErrorLocation(String query, ErrorLocation location, PrintStream out)
    {
        List<String> lines = ImmutableList.copyOf(Splitter.on('\n').split(query).iterator());

        String errorLine = lines.get(location.getLineNumber() - 1);
        String good = errorLine.substring(0, location.getColumnNumber() - 1);
        String bad = errorLine.substring(location.getColumnNumber() - 1);

        if ((location.getLineNumber() == lines.size()) && bad.trim().isEmpty()) {
            bad = " <EOF>";
        }

        if (REAL_TERMINAL) {
            Ansi ansi = Ansi.ansi();

            ansi.fg(Ansi.Color.CYAN);
            for (int i = 1; i < location.getLineNumber(); i++) {
                ansi.a(lines.get(i - 1)).newline();
            }
            ansi.a(good);

            ansi.fg(Ansi.Color.RED);
            ansi.a(bad).newline();
            for (int i = location.getLineNumber(); i < lines.size(); i++) {
                ansi.a(lines.get(i)).newline();
            }

            ansi.reset();
            out.println(ansi);
        }
        else {
            String prefix = format("LINE %s: ", location.getLineNumber());
            String padding = Strings.repeat(" ", prefix.length() + (location.getColumnNumber() - 1));
            out.println(prefix + errorLine);
            out.println(padding + "^");
        }
    }

    private static void renderStack(QueryResults results, PrintStream out)
    {
        if (results.getError().getFailureInfo() != null) {
            results.getError().getFailureInfo().toException().printStackTrace(out);
        }
    }
}
