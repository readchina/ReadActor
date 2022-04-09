# Design Drafts

Tool name: The current package name draft is `ReadChinaLookUp` which is fine: The Command name should be memorable so here are some suggestions (more welcome):

- `ReadChinaLookUp`
- `ReadActor`
- `SemBot`
- `RCL`
- `lookup`
- `lookup-bot`
- …

In the pseudocode below I ll use `tool` to denote the tool name.

## Commands

1. ` `
The main command, i.e. cross reference data from csv tables, with wikidata.  

```bash
tool  <path>
tool  ../data/
tool  https://github.com/readchina/ReadAct/tree/master/csv/data
```

Ideally the tool will be smart enough to determine the file name and know what info to look up based on that.

## Options

I generally don't like CLI programs which only support either `--` long or `-` short arguments. Our tool should support both forms. 

1. `-h` or `--help`
2. `-v` or `--version`
3. `-d` or `--debug`
   full log output to console
4. `-q` or `--quiet`
   no log output to console other then completion message and error level events
5. `-i` or `--interactive`
   do not update data, but prompt user for confirmation (should not be on by default)
6. `-f` or `--file`
   used to check a single table at `<path>` (must be a supported ReadAct file name). 
7. `-o` or `--output`
   do not update input table, but create a new file at `<path>` instead
8. `-s` or `--summary`
   do not update input table, but summarise results in console
9. `-a` or `--agents`
   process only agents (persons and institutions)
11.`-s` or `--space` 
   process only  places (places and locations)
12. `-c` or `--color`  
    use color in console output (default is false)

## Arguments

1. `<path>`
   It would be nice to design the path argument so that it understands both local paths and full URI to perform lookups.
2. 