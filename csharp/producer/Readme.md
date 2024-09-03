### Development environment

- .NET Framework 6.0 <br>
- dotnet add package OpenCvSharp4 --version 4.6.0.20220608 <br>
- dotnet add package OpenCvSharp4.runtime.osx.10.15-x64 --version 4.6.0.20220608 <br>
- dotnet add package Confluent.Kafka --version 1.9.3 <br>


### .csproj
```
<ItemGroup>
  <Reference Include="KafkaCSProducer">
    <HintPath>kafkacs.dll</HintPath>
  </Reference>
</ItemGroup>
```
