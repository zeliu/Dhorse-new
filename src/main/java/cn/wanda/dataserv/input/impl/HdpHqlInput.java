package cn.wanda.dataserv.input.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.regex.Pattern;

import lombok.extern.log4j.Log4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

import cn.wanda.dataserv.config.LocationConfig;
import cn.wanda.dataserv.config.el.expression.ExpressionUtils;
import cn.wanda.dataserv.config.location.HdpHqlInputLocation;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.core.ObjectFactory;
import cn.wanda.dataserv.engine.InputWorker;
import cn.wanda.dataserv.input.AbstractInput;
import cn.wanda.dataserv.input.InputException;
import cn.wanda.dataserv.utils.DFSUtils;
import cn.wanda.dataserv.utils.HiveUtils;
import cn.wanda.dataserv.utils.StringUtil;

@Log4j
public class HdpHqlInput extends AbstractInput {

    private HiveConf conf;

    private int ret = 0;

    private String hql;

    private SessionState ss;

    private Thread cmdSink;

    private BufferedWriter bw;

    private BufferedReader br;

    private static final Pattern pattern = Pattern.compile("(?<=\t|^)NULL(?=\t|$)");

    @Override
    public void init() {
        // process params
        LocationConfig l = this.inputConfig.getLocation();

        if (!HdpHqlInputLocation.class.isAssignableFrom(l.getClass())) {
            throw new InputException("input config type is not HQL!");
        }
        ClassLoader cl = ObjectFactory.dpumpClassLoader.get(HdpHqlInput.class.getName());
        if (cl != null) {
            Thread.currentThread().setContextClassLoader(cl);
        }

        DFSUtils.loadHadoopNative(DFSUtils.HHDFS);

        HdpHqlInputLocation location = (HdpHqlInputLocation) l;

        try {
            this.conf = HiveUtils.getConf(location.getHdfs().getUgi(), location
                    .getHive().getHiveConf(), location.getHdfs()
                    .getHadoopConf(), HiveUtils.HHIVE);

            this.hql = ExpressionUtils.getOneValue(location.getHql());

            PipedOutputStream pos = new PipedOutputStream();
            this.bw = new BufferedWriter(new OutputStreamWriter(pos, this.encoding));

            InputStream is = new PipedInputStream(pos);
            this.br = new BufferedReader(new InputStreamReader(is, this.encoding));

            this.cmdSink = new Thread() {

                public void run() {
                    try {
                        ss = new SessionState(conf);
                        SessionState.start(ss);

                        String command = "";
                        for (String oneCmd : hql.split(";")) {
                            // TODO : auth check
                            if (StringUtils.endsWith(oneCmd, "\\")) {
                                command += StringUtils.chop(oneCmd) + ";";
                                continue;
                            } else {
                                command += oneCmd;
                            }
                            if (StringUtils.isBlank(command)) {
                                continue;
                            }

                            String cmd_trimmed = command.trim();
                            String[] tokens = cmd_trimmed.split("\\s+");
                            CommandProcessor proc = CommandProcessorFactory.get(
                                    tokens, (HiveConf) conf);
                            processHql(command, proc, ss);
                            // wipe cli query state
                            ss.setCommandType(null);
                            command = "";
                            boolean ignoreErrors = HiveConf.getBoolVar(conf,
                                    HiveConf.ConfVars.CLIIGNOREERRORS);
                            if (ret != 0 && !ignoreErrors) {
                                CommandProcessorFactory.clean((HiveConf) conf);
                            }
                        }
                    } catch (Exception e) {
                        InputWorker.INPUT_ERROR.put(inputConfig.getId() + "_inner", e);
                    } finally {
                        // cleanup
                        try {
                            if (bw != null) {
                                bw.close();
                                bw = null;
                            }
                        } catch (IOException ex) {
                            InputWorker.INPUT_ERROR.put(inputConfig.getId() + "_inner_f", ex);
                        }
                    }
                }
            };

            this.cmdSink.start();
        } catch (IOException e) {
            throw new InputException("get hive conf failed.", e);
        }
    }

    private void processHql(String cmd, CommandProcessor proc, SessionState ss) {
        if (proc != null) {
            try {
                if (proc instanceof Driver) {
                    Driver qp = (Driver) proc;
                    log.info("run hql: " + cmd);
                    ret = qp.run(cmd).getResponseCode();
                    if (ret != 0) {
                        qp.close();
                        return;
                    }
                    ArrayList<String> res = new ArrayList<String>();

                    while (qp.getResults(res)) {
                        for (String r : res) {
                            bw.write(r);
                            bw.newLine();
                        }
                        bw.flush();
                        res.clear();
                    }


                    int cret = qp.close();
                    if (ret == 0) {
                        ret = cret;
                    }
                } else {
                    String firstToken = cmd.split("\\s+")[0];
                    String cmd_1 = cmd.trim().substring(firstToken.length()).trim();
                    if (ss.getIsVerbose()) {
                        log.info(firstToken + " " + cmd_1);
                    }
                    ret = proc.run(cmd_1).getResponseCode();
                }
            } catch (CommandNeedRetryException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Line readLine() {
        try {
            String line = br.readLine();
            if (StringUtils.isNotBlank(line)) {
                return new Line(pattern.matcher(line).replaceAll(StringUtil.EMPTY));
            } else {
                return Line.EOF;
            }
        } catch (IOException e) {
            throw new InputException(e);
        }
    }

    @Override
    public void close() {
        if (null != this.cmdSink) {
            while (true) {
                try {
                    this.cmdSink.join();
                } catch (InterruptedException ie) {
                    // interrupted; loop around.
                    log.debug(ie.getMessage(), ie);
                    continue;
                }

                break;
            }
        }
        try {
            if (bw != null) {
                bw.close();
                bw = null;
            }
            if (br != null) {
                br.close();
                br = null;
            }
        } catch (IOException ex) {
            throw new InputException("close hql input failed.", ex);
        }
    }

}