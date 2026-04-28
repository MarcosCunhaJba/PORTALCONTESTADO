@echo off
chcp 65001 >nul
echo Criando regra de firewall para porta 8090...
netsh advfirewall firewall add rule name="CH Contestado Sistema Local 8090" dir=in action=allow protocol=TCP localport=8090 profile=private
echo Pronto. Use somente em rede privada/confiavel.
pause
