import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, TextInput, View } from 'react-native';
import { useTranslation } from 'react-i18next';
import { useSafeAreaInsets } from 'react-native-safe-area-context';
import type { AgentAdapter } from '@clawket/agent-protocol';
import type { NanobotModelPreset, NanobotModelSettings, NanobotProviderRow } from '@clawket/agent-protocol';
import { AccountSettingsPageHeader } from '../AccountSettings/AccountSettingsPageHeader';
import { SettingsDivider, SettingsGroup, SettingsRow } from '../../components/ui/SettingsGroup';
import { Button } from '../../components/ui/Button';
import { Sheet } from '../../components/ui/Sheet';
import { useAppTheme } from '../../theme';
import { FontSize, FontWeight, LineHeight, Radius, Space } from '../../theme/tokens';

type Props = Readonly<{
  adapter: AgentAdapter;
  onBack: () => void;
}>;

type PresetDraft = { name: string; model: string; provider: string; max_tokens: string; context_window_tokens: string; temperature: string; reasoning_effort: string };
type ProviderDraft = { name: string; api_base: string; api_key: string; proxy: string };
type ParamDraft = { max_tokens: string; context_window_tokens: string; temperature: string; reasoning_effort: string };

const EMPTY_PRESET: PresetDraft = { name: '', model: '', provider: '', max_tokens: '', context_window_tokens: '', temperature: '', reasoning_effort: '' };
const EMPTY_PROVIDER: ProviderDraft = { name: '', api_base: '', api_key: '', proxy: '' };
const EMPTY_PARAM: ParamDraft = { max_tokens: '', context_window_tokens: '', temperature: '', reasoning_effort: '' };

function compact(input: Record<string, string>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(input)) {
    const trimmed = value.trim();
    if (trimmed) out[key] = trimmed;
  }
  return out;
}

export function NanobotModelsScreen({ adapter, onBack }: Props): React.JSX.Element {
  const { t } = useTranslation(['settings', 'common']);
  const { theme: { colors } } = useAppTheme();
  const insets = useSafeAreaInsets();
  const ops = adapter.management?.nanobotModels;

  const [settings, setSettings] = useState<NanobotModelSettings | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [orderDraft, setOrderDraft] = useState<string[]>([]);

  const [presetDraft, setPresetDraft] = useState<PresetDraft | null>(null);
  const [providerDraft, setProviderDraft] = useState<ProviderDraft | null>(null);
  const [paramDraft, setParamDraft] = useState<ParamDraft | null>(null);

  const styles = useMemo(() => createStyles(colors), [colors]);

  const reload = useCallback(async () => {
    if (!ops?.readSettings) { setError('Model settings are unavailable on this connection.'); setLoading(false); return; }
    setLoading(true);
    try {
      const data = await ops.readSettings();
      setSettings(data);
      setOrderDraft(data.callOrder);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load model settings.');
    } finally {
      setLoading(false);
    }
  }, [ops]);

  useEffect(() => { void reload(); }, [reload]);

  const run = useCallback(async (action: () => Promise<unknown>) => {
    setBusy(true);
    try { await action(); await reload(); setError(null); }
    catch (err) { setError(err instanceof Error ? err.message : 'Request failed.'); }
    finally { setBusy(false); }
  }, [reload]);

  const submitPreset = useCallback(async () => {
    if (!presetDraft || !ops) return;
    const body = compact({
      name: presetDraft.name, model: presetDraft.model, provider: presetDraft.provider,
      max_tokens: presetDraft.max_tokens, context_window_tokens: presetDraft.context_window_tokens,
      temperature: presetDraft.temperature, reasoning_effort: presetDraft.reasoning_effort,
    });
    const editing = Boolean(settings?.presets.find((p) => p.name === presetDraft.name && !p.isDefault));
    setPresetDraft(null);
    await run(() => (editing ? ops.updatePreset?.(body) : ops.createPreset?.(body)) ?? Promise.resolve());
  }, [ops, presetDraft, run, settings]);

  const submitProvider = useCallback(async () => {
    if (!providerDraft || !ops) return;
    const body = compact({ name: providerDraft.name, display_name: providerDraft.name, api_base: providerDraft.api_base, api_key: providerDraft.api_key, proxy: providerDraft.proxy });
    const editing = Boolean(settings?.providers.find((p) => p.name === providerDraft.name));
    setProviderDraft(null);
    await run(() => (editing ? ops.updateProvider?.(body) : ops.createProvider?.(body)) ?? Promise.resolve());
  }, [ops, providerDraft, run, settings]);

  const submitParams = useCallback(async () => {
    if (!paramDraft || !ops) return;
    const body = compact({ max_tokens: paramDraft.max_tokens, context_window_tokens: paramDraft.context_window_tokens, temperature: paramDraft.temperature, reasoning_effort: paramDraft.reasoning_effort });
    setParamDraft(null);
    await run(() => ops.updateAgentModel?.(body) ?? Promise.resolve());
  }, [ops, paramDraft, run]);

  const moveOrder = useCallback((index: number, delta: number) => {
    setOrderDraft((current) => {
      const target = index + delta;
      if (target < 0 || target >= current.length) return current;
      const next = [...current];
      const [item] = next.splice(index, 1);
      next.splice(target, 0, item);
      return next;
    });
  }, []);

  const field = (label: string, value: string, onChange: (text: string) => void, opts: { numeric?: boolean; secure?: boolean } = {}) => (
    <View style={styles.field}>
      <Text style={styles.fieldLabel}>{label}</Text>
      <TextInput
        style={styles.input}
        value={value}
        onChangeText={onChange}
        placeholderTextColor={colors.inkTertiary}
        keyboardType={opts.numeric ? 'numeric' : 'default'}
        secureTextEntry={opts.secure === true}
        autoCapitalize="none"
        autoCorrect={false}
      />
    </View>
  );

  const header = <AccountSettingsPageHeader testID="nanobot-models" title={t('Models', { ns: 'common' })} onBack={onBack} />;

  if (loading && !settings) {
    return <View style={[styles.screen, { backgroundColor: colors.canvasGrouped }]}>{header}<View style={styles.center}><ActivityIndicator color={colors.inkSecondary} /></View></View>;
  }

  return (
    <View style={[styles.screen, { backgroundColor: colors.canvasGrouped }]}>
      {header}
      <ScrollView contentContainerStyle={[styles.content, { paddingBottom: insets.bottom + Space.xl }]}>
        {error ? <Text style={styles.error}>{error}</Text> : null}

        <Text style={styles.sectionTitle}>{t('Default model')}</Text>
        <SettingsGroup density="comfortable">
          <SettingsRow testID="nanobot-model-default" title={settings?.agent.model || '—'} value={settings?.agent.resolvedProvider || settings?.agent.provider} />
          <SettingsDivider inset="content" />
          <SettingsRow testID="nanobot-model-params" title={t('Sampling parameters')} showChevron disabled={busy || !settings}
            onPress={() => setParamDraft({
              max_tokens: settings?.agent.maxTokens != null ? String(settings.agent.maxTokens) : '',
              context_window_tokens: settings?.agent.contextWindowTokens != null ? String(settings.agent.contextWindowTokens) : '',
              temperature: settings?.agent.temperature != null ? String(settings.agent.temperature) : '',
              reasoning_effort: settings?.agent.reasoningEffort ?? '',
            })} />
        </SettingsGroup>

        <Text style={styles.sectionTitle}>{t('Model configurations')}</Text>
        <SettingsGroup density="comfortable">
          {(settings?.presets ?? []).map((preset, index) => (
            <React.Fragment key={preset.name || index}>
              {index > 0 ? <SettingsDivider inset="content" /> : null}
              <SettingsRow
                testID={`nanobot-preset-${preset.name}`}
                title={preset.label || preset.name}
                value={preset.isDefault ? t('Default') : preset.model}
                subtitle={preset.isDefault ? preset.model : undefined}
                showChevron={!preset.active && !preset.isDefault}
                disabled={busy}
                onPress={() => { if (!preset.active) void run(() => ops?.updateAgentModel?.({ model_preset: preset.name }) ?? Promise.resolve()); }}
              />
              {!preset.isDefault ? (
                <View style={styles.rowActions}>
                  <Pressable disabled={busy} onPress={() => setPresetDraft({ name: preset.name, model: preset.model, provider: preset.provider, max_tokens: preset.maxTokens != null ? String(preset.maxTokens) : '', context_window_tokens: preset.contextWindowTokens != null ? String(preset.contextWindowTokens) : '', temperature: preset.temperature != null ? String(preset.temperature) : '', reasoning_effort: preset.reasoningEffort ?? '' })}>
                    <Text style={styles.link}>{t('Edit', { ns: 'common' })}</Text>
                  </Pressable>
                  <Pressable disabled={busy} onPress={() => void run(() => ops?.deletePreset?.(preset.name) ?? Promise.resolve())}>
                    <Text style={styles.danger}>{t('Delete', { ns: 'common' })}</Text>
                  </Pressable>
                </View>
              ) : null}
            </React.Fragment>
          ))}
        </SettingsGroup>
        <Button label={t('Add model configuration')} variant="card" disabled={busy} onPress={() => setPresetDraft({ ...EMPTY_PRESET })} />

        <Text style={styles.sectionTitle}>{t('Call order')}</Text>
        <SettingsGroup density="comfortable">
          {orderDraft.length === 0 ? (
            <SettingsRow title={t('No fallback order')} />
          ) : orderDraft.map((name, index) => (
            <React.Fragment key={`${name}-${index}`}>
              {index > 0 ? <SettingsDivider inset="content" /> : null}
              <SettingsRow
                testID={`nanobot-order-${name}`}
                title={`${index + 1}. ${name}`}
                subtitle={index === 0 ? t('Primary') : t('Fallback')}
              />
              <View style={styles.rowActions}>
                <Pressable disabled={busy || index === 0} onPress={() => moveOrder(index, -1)}><Text style={styles.link}>↑</Text></Pressable>
                <Pressable disabled={busy || index === orderDraft.length - 1} onPress={() => moveOrder(index, 1)}><Text style={styles.link}>↓</Text></Pressable>
              </View>
            </React.Fragment>
          ))}
        </SettingsGroup>
        {orderDraft.length > 0 && settings?.callOrderEditable !== false ? (
          <Button label={t('Save call order')} variant="card" disabled={busy} onPress={() => void run(() => ops?.updateCallOrder?.(orderDraft) ?? Promise.resolve())} />
        ) : null}

        <Text style={styles.sectionTitle}>{t('Providers')}</Text>
        <SettingsGroup density="comfortable">
          {(settings?.providers ?? []).map((provider: NanobotProviderRow, index) => (
            <React.Fragment key={provider.name || index}>
              {index > 0 ? <SettingsDivider inset="content" /> : null}
              <SettingsRow
                testID={`nanobot-provider-${provider.name}`}
                title={provider.label || provider.name}
                value={provider.configured ? t('Configured') : t('Not configured')}
                subtitle={provider.apiBase || provider.defaultApiBase || undefined}
                showChevron
                disabled={busy}
                onPress={() => setProviderDraft({ name: provider.name, api_base: provider.apiBase ?? '', api_key: '', proxy: provider.proxy ?? '' })}
              />
            </React.Fragment>
          ))}
        </SettingsGroup>
        <Button label={t('Add provider')} variant="card" disabled={busy} onPress={() => setProviderDraft({ ...EMPTY_PROVIDER })} />
      </ScrollView>

      <Sheet testID="nanobot-preset-sheet" visible={presetDraft !== null} title={t('Model configuration')} onClose={() => setPresetDraft(null)} closeAccessibilityLabel={t('Close', { ns: 'common' })} maxHeight="85%">
        {presetDraft ? (
          <ScrollView contentContainerStyle={styles.sheetBody}>
            {field(t('Name'), presetDraft.name, (v) => setPresetDraft({ ...presetDraft, name: v }))}
            {field(t('Model'), presetDraft.model, (v) => setPresetDraft({ ...presetDraft, model: v }))}
            {field(t('Provider'), presetDraft.provider, (v) => setPresetDraft({ ...presetDraft, provider: v }))}
            {field('max_tokens', presetDraft.max_tokens, (v) => setPresetDraft({ ...presetDraft, max_tokens: v }), { numeric: true })}
            {field('context_window_tokens', presetDraft.context_window_tokens, (v) => setPresetDraft({ ...presetDraft, context_window_tokens: v }), { numeric: true })}
            {field('temperature', presetDraft.temperature, (v) => setPresetDraft({ ...presetDraft, temperature: v }), { numeric: true })}
            {field('reasoning_effort', presetDraft.reasoning_effort, (v) => setPresetDraft({ ...presetDraft, reasoning_effort: v }))}
            <Button label={t('Save', { ns: 'common' })} variant="primary" loading={busy} onPress={() => void submitPreset()} />
          </ScrollView>
        ) : null}
      </Sheet>

      <Sheet testID="nanobot-provider-sheet" visible={providerDraft !== null} title={t('Provider')} onClose={() => setProviderDraft(null)} closeAccessibilityLabel={t('Close', { ns: 'common' })} maxHeight="85%">
        {providerDraft ? (
          <ScrollView contentContainerStyle={styles.sheetBody}>
            {field(t('Name'), providerDraft.name, (v) => setProviderDraft({ ...providerDraft, name: v }))}
            {field('api_base', providerDraft.api_base, (v) => setProviderDraft({ ...providerDraft, api_base: v }))}
            {field('api_key', providerDraft.api_key, (v) => setProviderDraft({ ...providerDraft, api_key: v }), { secure: true })}
            {field('proxy', providerDraft.proxy, (v) => setProviderDraft({ ...providerDraft, proxy: v }))}
            <Button label={t('Save', { ns: 'common' })} variant="primary" loading={busy} onPress={() => void submitProvider()} />
          </ScrollView>
        ) : null}
      </Sheet>

      <Sheet testID="nanobot-params-sheet" visible={paramDraft !== null} title={t('Sampling parameters')} onClose={() => setParamDraft(null)} closeAccessibilityLabel={t('Close', { ns: 'common' })} maxHeight="85%">
        {paramDraft ? (
          <ScrollView contentContainerStyle={styles.sheetBody}>
            {field('max_tokens', paramDraft.max_tokens, (v) => setParamDraft({ ...paramDraft, max_tokens: v }), { numeric: true })}
            {field('context_window_tokens', paramDraft.context_window_tokens, (v) => setParamDraft({ ...paramDraft, context_window_tokens: v }), { numeric: true })}
            {field('temperature', paramDraft.temperature, (v) => setParamDraft({ ...paramDraft, temperature: v }), { numeric: true })}
            {field('reasoning_effort', paramDraft.reasoning_effort, (v) => setParamDraft({ ...paramDraft, reasoning_effort: v }))}
            <Button label={t('Save', { ns: 'common' })} variant="primary" loading={busy} onPress={() => void submitParams()} />
          </ScrollView>
        ) : null}
      </Sheet>
    </View>
  );
}

function createStyles(colors: ReturnType<typeof useAppTheme>['theme']['colors']) {
  return StyleSheet.create({
    screen: { flex: 1 },
    content: { paddingHorizontal: Space.lg, gap: Space.md },
    center: { flex: 1, alignItems: 'center', justifyContent: 'center' },
    sectionTitle: { color: colors.inkSecondary, fontSize: FontSize.secondary, fontWeight: FontWeight.semibold, marginTop: Space.lg, marginBottom: Space.xs },
    error: { color: colors.warn, fontSize: FontSize.secondary, lineHeight: LineHeight.secondary, paddingVertical: Space.sm },
    rowActions: { flexDirection: 'row', gap: Space.lg, paddingHorizontal: Space.lg, paddingBottom: Space.sm },
    link: { color: colors.inkSecondary, fontSize: FontSize.secondary, fontWeight: FontWeight.semibold },
    danger: { color: colors.warn, fontSize: FontSize.secondary, fontWeight: FontWeight.semibold },
    sheetBody: { padding: Space.lg, gap: Space.md },
    field: { gap: Space.xs },
    fieldLabel: { color: colors.inkSecondary, fontSize: FontSize.secondary, fontWeight: FontWeight.regular },
    input: { color: colors.ink, fontSize: FontSize.body, borderWidth: 1, borderColor: colors.surface, borderRadius: Radius.md, paddingHorizontal: Space.md, paddingVertical: Space.sm, backgroundColor: colors.surfaceFloating },
  });
}
