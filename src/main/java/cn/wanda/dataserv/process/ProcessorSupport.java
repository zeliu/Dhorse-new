package cn.wanda.dataserv.process;


public abstract class ProcessorSupport implements Processor {

    protected LineSchema schema;

    public ProcessorSupport() {
    }

    @Override
    public LineSchema process(LineSchema lineSchema) {
        this.schema = lineSchema;
        LineSchema schemaAfter;
        schemaAfter = lineSchema.clone();
        return processSchema(schemaAfter);
    }

    protected abstract LineSchema processSchema(LineSchema schemaAfter);

    @Override
    public LineWrapper process(LineWrapper line) {
        return processLine(line);
    }

    protected abstract LineWrapper processLine(LineWrapper line);

}
