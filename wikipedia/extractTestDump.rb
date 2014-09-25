i = 0

File.open("extractedDumpLines.xml", "w+") do |testDump|
  File.open("deWikipedia.xml") do |dump|
    loop do
      line = dump.readline
      testDump << line
      i += 1
      if i > 2000
        exit
      end
    end
  end
end